import asyncio
import json
import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import paho.mqtt.client as mqtt
import uvicorn

# --- Configuration ---
# All messages are now expected from a single topic.
MQTT_BROKER_URL = os.getenv("MQTT_BROKER_URL", "10.233.46.175")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", 1883))
MQTT_INPUT_TOPIC = os.getenv("REACT_APP_MQTT_TOPIC", "spd_events_pm_demo")

# --- FastAPI App ---
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ConnectionManager:
    """Manages active WebSocket connections."""
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

manager = ConnectionManager()

# --- MQTT Client Logic ---
async def on_message_handler(topic, payload):
    """
    Parses incoming MQTT messages from a single topic, determines the message type
    based on the 'Event' field in the payload, and broadcasts them via WebSocket.
    """
    print(f"Received message from topic {topic}")
    
    message_to_send = None
    try:
        # First, decode the payload as a JSON object.
        data = json.loads(payload)
        
        # Check for the 'Event' field to determine message type.
        event_type = data.get("Event")

        if event_type == 'SPD':
            # If Event is 'spd', this is a 'tag' message.
            message_to_send = json.dumps({"type": "tag", "payload": data})
        elif event_type == 'RAW':
            # If Event is 'raw', this is a 'report' message.
            message_to_send = json.dumps({"type": "report", "payload": data})
        else:
            print(f"Warning: Unknown 'Event' type in message: {payload}")
            return

    except json.JSONDecodeError:
        print(f"Warning: Could not decode message as JSON: {payload}")
        return
    except KeyError:
        print(f"Warning: 'Event' key not found in message payload: {payload}")
        return

    if message_to_send:
        await manager.broadcast(message_to_send)

def on_connect(client, userdata, flags, rc):
    """The callback for when the client receives a CONNACK response from the server."""
    if rc == 0:
        print("Connected to MQTT Broker!")
        # Subscribe to the single input topic upon connection.
        client.subscribe(MQTT_INPUT_TOPIC, 1)
    else:
        print(f"Failed to connect to MQTT, return code {rc}\\n")

def on_message(client, userdata, msg):
    """
    The callback for when a PUBLISH message is received from the server.
    It schedules the async handler on the main event loop.
    """
    loop = userdata['loop']
    asyncio.run_coroutine_threadsafe(on_message_handler(msg.topic, msg.payload.decode()), loop)

def setup_mqtt_client(loop):
    """Creates and configures the MQTT client."""
    client = mqtt.Client(userdata={'loop': loop})
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER_URL, MQTT_BROKER_PORT, 60)
    return client

# --- FastAPI Events & Endpoints ---
@app.on_event("startup")
async def startup_event():
    """On server startup, set up and start the MQTT client."""
    print("Starting server and MQTT client...")
    loop = asyncio.get_running_loop()
    mqtt_client = setup_mqtt_client(loop)
    mqtt_client.loop_start()  # Starts a background thread for the MQTT network loop
    app.state.mqtt_client = mqtt_client
    print(f"WebSocket endpoint is ready at ws://127.0.0.1:8000/ws")
    print(f"Subscribing to MQTT topic on {MQTT_BROKER_URL}:")
    print(f" - Topic: {MQTT_INPUT_TOPIC}")

@app.on_event("shutdown")
def shutdown_event():
    """On server shutdown, stop the MQTT client."""
    print("Shutting down MQTT client...")
    app.state.mqtt_client.loop_stop()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """The WebSocket endpoint that React will connect to."""
    await manager.connect(websocket)
    try:
        while True:
            # Keep the connection alive to receive messages.
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        print("Client disconnected from WebSocket.")

# --- Main Execution ---
if __name__ == "__main__":
    # Use this to run the server directly with `python main.py`
    uvicorn.run(app, host="127.0.0.1", port=8001)


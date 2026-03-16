# WebSocket Server Setup Instructions

This project uses a FastAPI WebSocket server to connect to an MQTT broker and stream messages to the React frontend.

## Prerequisites

Install required Python packages:

```bash
pip install fastapi uvicorn paho-mqtt
```

## Running the WebSocket Server

Before starting the React app, start the WebSocket server in a separate terminal:

```bash
cd demov2
python websocket.py
```

You should see output like:
```
Starting server and MQTT client...
Connected to MQTT Broker!
WebSocket endpoint is ready at ws://127.0.0.1:8001/ws
Subscribing to MQTT topic on 10.233.46.175:
 - Topic: spd_events_pm_demo
```

## Configuration

Edit `websocket.py` to change MQTT settings:

- `MQTT_BROKER_URL`: Default is "10.233.46.175"
- `MQTT_BROKER_PORT`: Default is 1883
- `MQTT_INPUT_TOPIC`: Default is "spd_events_pm_demo"

Or set environment variables:
```bash
set MQTT_BROKER_URL=your_broker_ip
set MQTT_BROKER_PORT=1883
set REACT_APP_MQTT_TOPIC=your_topic
```

## Running the React App

In another terminal:

```bash
cd demov2
npm start
```

## How It Works

1. Click **Start** button → React connects to WebSocket at `localhost:8001`
2. WebSocket server connects to MQTT broker and subscribes to the configured topic
3. Messages arrive → Server broadcasts to all connected WebSocket clients
4. Frontend receives and displays messages in real-time
5. Click **Stop** button → Disconnect from WebSocket

## Message Format

Messages from MQTT are processed based on the `Event` field:
- `Event: 'SPD'` → Broadcast as `{type: "tag", payload: {...}}`
- `Event: 'RAW'` → Broadcast as `{type: "report", payload: {...}}`

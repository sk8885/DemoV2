import os
import sys
import json
import time
import pandas as pd
import subprocess
from datetime import datetime
import paho.mqtt.client as mqttclient
import shutil
from dotenv import load_dotenv
import asyncio
import ssl
import threading
import websockets
from pydantic import BaseModel, ValidationError
from typing import Literal

load_dotenv()

# --- Pydantic Models for Tag Data Validation ---

class TagData(BaseModel):
    """
    Pydantic model to validate the nested 'data' object in a tag read message.
    It ensures all required fields are present.
    """
    antenna: int
    channel: float
    eventNum: int
    format: str
    idHex: str
    peakRssi: int
    phase: float
    reads: int

class TagRead(BaseModel):
    """
    Pydantic model to validate the overall structure of an incoming tag read message.
    """
    data: TagData
    timestamp: str
    type: Literal['CUSTOM']

# --- Environment and Path Setup ---

# Access the environment variables
broker_address = os.getenv('MQTT_BROKER_ADDRESS')
port = int(os.getenv('MQTT_PORT'))
mqtt_data_topic = os.getenv('MQTT_DATA_TOPIC')
mqtt_command_topic = os.getenv('MQTT_COMMAND_TOPIC')
mqtt_response_topic = os.getenv('MQTT_RESPONSE_TOPIC')
websocket_url = os.getenv('WEBSOCKET_URL')

Connected = False
validate_flag = False
dataList = []
use_websocket = False


# Set up directories and file paths
current_date = datetime.now().strftime("%d-%m-%Y")
file_path = f"../../data/input/{current_date}"
# Ensure the daily storage directory exists before file writes.
os.makedirs(file_path, exist_ok=True)
save_file = f"{file_path}/save.txt"
csv_file = f"{file_path}/rawDataset.csv"
log_file = f"{file_path}/terminalLog.txt"
mqtt_validation_file = f"{file_path}/mqttValidation.txt"
data_update_file = f"{file_path}/dataUpdate.txt"


# --- Core Functions ---

def logFn(logtext):
    """Logs text to terminalLog.txt file and also prints it in the terminal window."""
    with open(log_file, 'a') as f:
        f.write(logtext)
    print(logtext)

def on_connect(client, userdata, flags, rc):
    """Callback which is triggered on connection acknowledgement."""
    if rc == 0:
        with open(mqtt_validation_file, 'w') as f:
            f.write("Connection successful")
        global Connected
        Connected = True
        logFn("Connected to broker.\n")
    else:
        with open(mqtt_validation_file, 'w') as f:
            f.write("Connection failed")
        logFn("Connection Failed\n")

def on_message(client, userdata, message):
    """Callback triggered on every message recieved."""
    validate_fn(message.payload.decode('utf-8'))

def on_publish(client, userdata, mid):
    """Call triggered on every publish."""
    print('Message published')
    pass

def validate_fn(message_payload):
    """
    Based on the validate flag, this function decides what action to be performed.
    Pydantic validation is now ONLY for incoming tag data.
    """
    global validate_flag
    if not validate_flag:
        # Original logic for command responses (no Pydantic)
        reader_response = json.loads(message_payload)
        logFn(f"command: {reader_response['command']}; response: {reader_response['response']}\n")
        if reader_response['response'] != "success":
            with open(mqtt_validation_file, 'w') as f:
                f.write(f"command: {reader_response['command']}; response: {reader_response['response']}")
            return
        if reader_response['command'] == "start":
            validate_flag = True
    else:
        # Pydantic validation for incoming tag data
        try:
            # Parse the incoming JSON
            message_json = json.loads(message_payload)
            # Validate the structure against our TagRead model
            TagRead.parse_obj(message_json)
            # If validation succeeds, add the original payload to the list
            global dataList
            dataList.append(message_payload)
        except (ValidationError, json.JSONDecodeError) as e:
            # If validation fails, log the error and the problematic data
            logFn(f"Tag data validation error: {e}\nPayload: {message_payload}\n")


def initialize_mqtt():
    """Inistialize the MQTT client."""
    client = mqttclient.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_publish = on_publish
    try:
        client.connect(broker_address, port=port)
    except Exception as e:
        with open(mqtt_validation_file, 'w') as f:
            f.write("Connection failed due to invalid mqtt broker")
        logFn("Connection failed due to invalid mqtt broker.\n")
        sys.exit()
    client.loop_start()
    return client


def get_inventory_commands():
    """
    Returns the inventory command definitions as plain dictionaries.
    (Reverted to original implementation).
    """
    stop_command = {
        "command": "stop",
        "command_id": "1",
        "payload": {}
    }
    set_mode_command = {
        "command": "set_mode",
        "command_id": "2",
        "payload":
        {
            "type": "CUSTOM",
            "antennas": [
                1, 2, 3, 4, 1, 2, 3, 4,
                1, 2, 3, 4, 1, 2, 3, 4
            ],
            "query": [
                {"session": "S2", "sel": "ALL", "target": "A", "tagPopulation": 500},
                {"session": "S3", "sel": "ALL", "target": "A", "tagPopulation": 500},
                {"session": "S3", "sel": "ALL", "target": "B", "tagPopulation": 500},
                {"session": "S2", "sel": "ALL", "target": "B", "tagPopulation": 500},
                {"session": "S1", "sel": "ALL", "target": "A", "tagPopulation": 500},
                {"session": "S1", "sel": "ALL", "target": "A", "tagPopulation": 500},
                {"session": "S1", "sel": "ALL", "target": "A", "tagPopulation": 500},
                {"session": "S1", "sel": "ALL", "target": "A", "tagPopulation": 500},
                {"session": "S3", "sel": "ALL", "target": "B", "tagPopulation": 500},
                {"session": "S2", "sel": "ALL", "target": "B", "tagPopulation": 500},
                {"session": "S2", "sel": "ALL", "target": "A", "tagPopulation": 500},
                {"session": "S3", "sel": "ALL", "target": "A", "tagPopulation": 500},
                {"session": "S1", "sel": "ALL", "target": "A", "tagPopulation": 500},
                {"session": "S1", "sel": "ALL", "target": "A", "tagPopulation": 500},
                {"session": "S1", "sel": "ALL", "target": "A", "tagPopulation": 500},
                {"session": "S1", "sel": "ALL", "target": "A", "tagPopulation": 500}
            ],
            "antennaStopCondition": [
                {"type": "SINGLE_INVENTORY_LIMITED_DURATION", "value": 50},
                {"type": "SINGLE_INVENTORY_LIMITED_DURATION", "value": 50},
                {"type": "SINGLE_INVENTORY_LIMITED_DURATION", "value": 50},
                {"type": "SINGLE_INVENTORY_LIMITED_DURATION", "value": 50},
                {"type": "SINGLE_INVENTORY_LIMITED_DURATION", "value": 150},
                {"type": "SINGLE_INVENTORY_LIMITED_DURATION", "value": 150},
                {"type": "SINGLE_INVENTORY_LIMITED_DURATION", "value": 150},
                {"type": "SINGLE_INVENTORY_LIMITED_DURATION", "value": 150},
                {"type": "SINGLE_INVENTORY_LIMITED_DURATION", "value": 50},
                {"type": "SINGLE_INVENTORY_LIMITED_DURATION", "value": 50},
                {"type": "SINGLE_INVENTORY_LIMITED_DURATION", "value": 50},
                {"type": "SINGLE_INVENTORY_LIMITED_DURATION", "value": 50},
                {"type": "SINGLE_INVENTORY_LIMITED_DURATION", "value": 150},
                {"type": "SINGLE_INVENTORY_LIMITED_DURATION", "value": 150},
                {"type": "SINGLE_INVENTORY_LIMITED_DURATION", "value": 150},
                {"type": "SINGLE_INVENTORY_LIMITED_DURATION", "value": 150}
            ],
            "tagMetaData": ["RSSI", "PHASE", "SEEN_COUNT", "ANTENNA", "CHANNEL"],
            "transmitPower": 30,
            "linkProfile": 201,
            "reportFilter": {"duration": 0, "type": "RADIO_WIDE"}
        }
    }
    start_command = {
        "command": "start",
        "command_id": "3",
        "payload": {"doNotPersistState": True}
    }
    return stop_command, set_mode_command, start_command


# --- WebSocket Fallback Functions ---

_ws_connected = threading.Event()
_ws_failed = threading.Event()
_ws_stop = threading.Event()

async def _ws_run(uri):
    """Async coroutine: connect to the reader WebSocket and collect data."""
    global dataList
    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    decoder = json.JSONDecoder()

    try:
        async with websockets.connect(uri, ssl=ssl_ctx) as ws:
            _ws_connected.set()
            while not _ws_stop.is_set():
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    if message:
                        if isinstance(message, (bytes, bytearray)):
                            message = message.decode('utf-8')

                        text = message.strip()
                        idx = 0
                        while idx < len(text):
                            try:
                                obj, end = decoder.raw_decode(text, idx)
                                # Validate with Pydantic and only store valid tag reads
                                tag_read = TagRead.parse_obj(obj)
                                if tag_read.type == 'CUSTOM':
                                    dataList.append(json.dumps(obj))
                                idx = end
                                while idx < len(text) and text[idx] in ' \t\r\n':
                                    idx += 1
                            except (json.JSONDecodeError, ValidationError) as e:
                                logFn(f"WS data validation error: {e}\nPayload: {text[idx:]}\n")
                                break
                except asyncio.TimeoutError:
                    continue
    except Exception as e:
        logFn(f"WebSocket error: {e}\n")
        _ws_failed.set()
    finally:
        _ws_connected.set()

def _ws_thread_fn(uri):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_ws_run(uri))
    loop.close()

def start_websocket_collection():
    """Fallback: connect to reader via WebSocket to listen for data."""
    global use_websocket
    logFn("Falling back to WebSocket connection...\n")
    _ws_connected.clear()
    _ws_failed.clear()
    _ws_stop.clear()
    t = threading.Thread(target=_ws_thread_fn, args=(websocket_url,), daemon=True)
    t.start()
    _ws_connected.wait(timeout=5)
    if _ws_failed.is_set() or not _ws_connected.is_set():
        logFn("WebSocket connection failed.\n")
        with open(mqtt_validation_file, 'w') as f:
            f.write("WebSocket connection failed")
        return None
    logFn(f"Connected to reader via WebSocket at {websocket_url}.\n")
    logFn("Please start the inventory manually on the reader.\n")
    print("\n*** Please start the inventory manually on the reader. ***\n")
    use_websocket = True
    return t

def stop_websocket_collection(ws):
    """Signal the background listener to stop and close the WebSocket."""
    _ws_stop.set()
    logFn("WebSocket connection closed.\n")
    print("\n*** Please stop the inventory manually on the reader. ***\n")


# --- Inventory Control Functions ---

def start_inventory(client):
    """Performs Stop, Set Mode, Start and validates IoT connection."""
    client.subscribe(mqtt_response_topic)
    stop_command, set_mode_command, start_command = get_inventory_commands()

    # Publish commands as simple JSON strings
    client.publish(mqtt_command_topic, payload=json.dumps(stop_command))
    client.publish(mqtt_command_topic, payload=json.dumps(set_mode_command))
    client.publish(mqtt_command_topic, payload=json.dumps(start_command))

    time.sleep(1)
    if not validate_flag:
        with open(mqtt_validation_file, 'w') as f:
            f.write("IoTConnector is disconnected in the reader")
        logFn("IoTConnector is disconnected in the reader.\n")
        return False
    client.unsubscribe(mqtt_response_topic)
    return True

def stop_inventory(client):
    """Stops the inventory after the testplan is executed."""
    global validate_flag
    validate_flag = False
    client.subscribe(mqtt_response_topic)
    stop_command = {
        "command": "stop",
        "command_id": "1",
        "payload": {}
    }
    client.publish(mqtt_command_topic, payload=json.dumps(stop_command))
    time.sleep(0.5)


# --- Data Processing and Cleanup Functions (Unchanged) ---

def add_to_csv(data_list, df):
    if data_list:
        try:
            dict_list = [json.loads(js) for js in data_list]
            flattened_list = []
            for item in dict_list:
                flat_data = item['data'].copy()
                flat_data['timestamp'] = item['timestamp']
                flat_data['type'] = item['type']
                flat_data['timestampConverted'] = convert_to_epoch(item['timestamp'])
                flattened_list.append(flat_data)
            
            new_data = pd.DataFrame(flattened_list)
            numberOfRows = len(new_data.axes[0])
            columns_csv = ['antenna', 'channel', 'eventNum', 'format', 'idHex', 'peakRssi', 'phase', 'reads', 'timestamp', 'type', 'timestampConverted']
            df = pd.concat([df, new_data], ignore_index=True)
            df.to_csv(f'{file_path}/rawDataset.csv', columns=columns_csv, index=False)
            logFn(f"{numberOfRows} rows of data saved.\n")
            return f"{numberOfRows} rows of data saved"
        except Exception as e:
            logFn(f"Failed to save CSV: {e}\n")
            return "Failed to save csv"
    else:
        logFn("No data to save.\n")
        return "No data to save"

def convert_to_csv(data_list):
    if data_list:
        try:
            dict_list = [json.loads(js) for js in data_list]
            flattened_list = []
            for item in dict_list:
                flat_data = item['data'].copy()
                flat_data['timestamp'] = item['timestamp']
                flat_data['type'] = item['type']
                flat_data['timestampConverted'] = convert_to_epoch(item['timestamp'])
                flattened_list.append(flat_data)
            
            df = pd.DataFrame(flattened_list)
            numberOfRows = len(df.axes[0])
            columns_csv = ['antenna', 'channel', 'eventNum', 'format', 'idHex', 'peakRssi', 'phase', 'reads', 'timestamp', 'type', 'timestampConverted']
            df.to_csv(f'{file_path}/rawDataset.csv', columns=columns_csv, index=False)
            logFn(f"{numberOfRows} rows of data saved.\n")
            return f"{numberOfRows} rows of data saved."
        except Exception as e:
            logFn(f"Failed to save CSV: {e}\n" )
            return f"Failed to save CSV: {e}."
    else:
        logFn("No data to save.\n")
        return "No data to save."

def convert_to_epoch(timestamp_str):
    try:
        datetime_object = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%f%z")
    except ValueError:
        datetime_object = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S%z")
    epoch_time = int(datetime_object.timestamp() * 1000)
    return epoch_time

def cleanup():
    audio_dir = "audio"
    if os.path.exists(audio_dir):
        shutil.rmtree(audio_dir)
    if os.path.exists(save_file):
        os.remove(save_file)
    if os.path.exists(mqtt_validation_file):
        os.remove(mqtt_validation_file)
    logFn("Clean up Successful...\n")


# --- Main Execution Block (Unchanged) ---

if __name__ == '__main__':
    logFn("\n")
    if os.path.exists(save_file):
        os.remove(save_file)

    logFn("MQTT execution started.\n")
    client = initialize_mqtt()
    mqtt_success = start_inventory(client)

    ws = None
    if mqtt_success:
        client.subscribe(mqtt_data_topic)
    else:
        logFn("MQTT commands failed. Attempting WebSocket fallback...\n")
        client.loop_stop()
        ws = start_websocket_collection()
        if ws is None:
            logFn("WebSocket fallback also failed. Exiting.\n")
            sys.exit()

    try:
        while True:
            time.sleep(1)
            if os.path.exists(save_file):
                with open(save_file, "r") as f:
                    if f.read().strip() == "saverows":
                        break
    except Exception as e:
        logFn(f"An error occured: {e}\n")
    finally:
        if use_websocket and ws:
            stop_websocket_collection(ws)
        else:
            client.unsubscribe(mqtt_data_topic)
            stop_inventory(client)
            time.sleep(1)
            client.loop_stop()

        if os.path.isfile(csv_file):
            df = pd.read_csv(f'{file_path}/rawDataset.csv')
            data_update_text = add_to_csv(dataList, df)
            with open(data_update_file, 'w') as f:
                f.write(f'{data_update_text}')
        else:
            data_update_text = convert_to_csv(dataList)
            with open(data_update_file, 'w') as f:
                f.write(f'{data_update_text}')

        cleanup()
        logFn(f"Testplan complete.\n")

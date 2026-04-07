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
import websockets
import requests

load_dotenv()

# Access the environment variables
broker_address = os.getenv('MQTT_BROKER_ADDRESS')
port = int(os.getenv('MQTT_PORT'))
mqtt_data_topic = os.getenv('MQTT_DATA_TOPIC')
websocket_uri = os.getenv('WEBSOCKET_URI')
reader_ip = os.getenv('READER_IP_ADDRESS') # Add your reader's IP to the .env file

Connected = False
dataList = []

# Set up directories and file paths
current_date = datetime.now().strftime("%d-%m-%Y")
file_path = f"../../data/input/{current_date}"
save_file = f"{file_path}/save.txt"
csv_file = f"{file_path}/rawDataset.csv"
log_file = f"{file_path}/terminalLog.txt"
data_update_file = f"{file_path}/dataUpdate.txt"

# Logs text to terminalLog.txt file and also prints it in the terminal window.
def logFn(logtext):
    with open(log_file, 'a') as f:
        f.write(logtext)
    print(logtext)

def is_reader_mqtt_connected():
    """Checks the reader's management endpoint to see if it's connected to MQTT."""
    if not reader_ip:
        logFn("READER_IP_ADDRESS not set in .env file. Skipping MQTT status check.\n")
        return False

    # The URL endpoint might vary based on the reader model and firmware.
    # This is a common endpoint for checking IoT Connector status on Zebra readers.
    status_url = f"http://{reader_ip}/v1/iot/status"
    logFn(f"Checking reader's MQTT connection status at: {status_url}\n")

    try:
        # This request checks the status of the cloud (MQTT) connector on the reader
        response = requests.get(status_url, timeout=5)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
        
        status_data = response.json()
        
        # The structure of the JSON response might vary.
        # We're checking for a state that indicates an active connection.
        if status_data.get('state') == 'CONNECTED':
            logFn("Reader's IoT Connector is CONNECTED to MQTT broker.\n")
            return True
        else:
            logFn(f"Reader's IoT Connector is not connected. Status: {status_data.get('state')}\n")
            return False

    except requests.exceptions.RequestException as e:
        logFn(f"Could not reach reader's management endpoint: {e}\n")
        return False
    except Exception as e:
        logFn(f"An unexpected error occurred during MQTT status check: {e}\n")
        return False


# Callback which is triggered on connection acknowledgement for MQTT
def on_connect(client, userdata, flags, rc):
    global Connected
    if rc == 0:
        Connected = True
        logFn("Successfully connected this script to MQTT broker.\n")
    else:
        logFn("This script failed to connect to MQTT broker.\n")

# Callback triggered on every message received for MQTT
def on_message(client, userdata, message):
    global dataList
    dataList.append(message.payload.decode('utf-8'))

# Inistialize the MQTT client
def initialize_mqtt():
    client = mqttclient.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    try:
        client.connect(broker_address, port=port)
        client.loop_start()
        time.sleep(2) # Wait for connection to establish
        return client
    except Exception as e:
        logFn(f"MQTT client connection failed: {e}\n")
        return None

# Function to collect data using WebSockets
async def collect_data_via_websocket():
    global dataList
    logFn(f"Attempting to connect to WebSocket at {websocket_uri}\n")
    try:
        async with websockets.connect(websocket_uri) as websocket:
            logFn("Connected to WebSocket.\n")
            # Set a timeout for receiving data
            while True:
                try:
                    # Check if the termination file exists
                    if os.path.exists(save_file):
                        with open(save_file, "r") as f:
                            if f.read().strip() == "saverows":
                                break
                    message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                    dataList.append(message)
                except asyncio.TimeoutError:
                    # This is now expected if no tags are read, continue the loop
                    continue
                except websockets.exceptions.ConnectionClosed:
                    logFn("WebSocket connection closed.\n")
                    break
    except Exception as e:
        logFn(f"WebSocket connection failed: {e}\n")

# Append the data to the current existing rawDataset.csv file
def add_to_csv(data_list, df):
    if data_list:
        try:
            # Handle both string and dict data gracefully
            dict_list = [json.loads(js) if isinstance(js, str) else js for js in data_list]
            flattened_list = []
            for item in dict_list:
                # Check for expected keys before processing
                if 'data' in item and 'timestamp' in item and 'type' in item:
                    flat_data = item['data'].copy()
                    flat_data['timestamp'] = item['timestamp']
                    flat_data['type'] = item['type']
                    flat_data['timestampConverted'] = convert_to_epoch(item['timestamp'])
                    flattened_list.append(flat_data)
                else:
                    logFn(f"Skipping malformed data item: {item}\n")

            if not flattened_list:
                logFn("No valid data to save after filtering.\n")
                return "No valid data to save"

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

# Create a rawDataset.csv file if it does not exist
def convert_to_csv(data_list):
    if data_list:
        try:
            # Handle both string and dict data gracefully
            dict_list = [json.loads(js) if isinstance(js, str) else js for js in data_list]
            flattened_list = []
            for item in dict_list:
                 # Check for expected keys before processing
                if 'data' in item and 'timestamp' in item and 'type' in item:
                    flat_data = item['data'].copy()
                    flat_data['timestamp'] = item['timestamp']
                    flat_data['type'] = item['type']
                    flat_data['timestampConverted'] = convert_to_epoch(item['timestamp'])
                    flattened_list.append(flat_data)
                else:
                    logFn(f"Skipping malformed data item: {item}\n")
            
            if not flattened_list:
                logFn("No valid data to save after filtering.\n")
                return "No valid data to save"

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

# Convert the timestamp to epoch time in milliseconds
def convert_to_epoch(timestamp_str):
    try:
        datetime_object = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%f%z")
    except ValueError:
        datetime_object = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S%z")
    epoch_time = int(datetime_object.timestamp() * 1000)
    return epoch_time

# Runs after the data collection is stopped
def cleanup():
    audio_dir = "audio"
    if os.path.exists(audio_dir):
        shutil.rmtree(audio_dir)

    if os.path.exists(save_file):
        os.remove(save_file)

    logFn("Clean up Successful...\n")

if __name__ == '__main__':
    logFn("\n")
    if os.path.exists(save_file):
        os.remove(save_file)

    # First, check reader's management endpoint for MQTT status
    use_mqtt = is_reader_mqtt_connected()
    client = None

    if use_mqtt:
        client = initialize_mqtt()
        if Connected:
            logFn("Proceeding with MQTT data collection.\n")
            client.subscribe(mqtt_data_topic)
            try:
                while True:
                    time.sleep(1)
                    # Check if the termination file exists
                    if os.path.exists(save_file):
                        with open(save_file, "r") as f:
                            if f.read().strip() == "saverows":
                                break
            except Exception as e:
                logFn(f"An error occurred during MQTT listening: {e}\n")
            finally:
                if client:
                    client.unsubscribe(mqtt_data_topic)
                    client.loop_stop()
        else:
            logFn("Failed to establish script connection to MQTT broker. Falling back to WebSocket.\n")
            asyncio.run(collect_data_via_websocket())
    else:
        logFn("Reader is not connected to MQTT broker. Falling back to WebSocket.\n")
        # Fallback to WebSocket
        asyncio.run(collect_data_via_websocket())

    # Save data collected from either MQTT or WebSocket
    if os.path.isfile(csv_file):
        try:
            df = pd.read_csv(f'{file_path}/rawDataset.csv')
            data_update_text = add_to_csv(dataList, df)
        except pd.errors.EmptyDataError:
             data_update_text = convert_to_csv(dataList) # file exists but is empty
    else:
        data_update_text = convert_to_csv(dataList)
    
    with open(data_update_file, 'w') as f:
        f.write(f'{data_update_text}')
    
    cleanup()
    logFn(f"Data collection complete.\n")

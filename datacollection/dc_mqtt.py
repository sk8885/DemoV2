import paho.mqtt.client as mqtt
import csv
import datetime
import threading
import json
from pathlib import Path

class MqttLogger:
    def __init__(self):
        # --- MQTT Configuration ---
        # PLEASE FILL IN YOUR MQTT BROKER DETAILS HERE
        self.mqtt_broker = "your_mqtt_broker_address"  # e.g., "mqtt.eclipseprojects.io"
        self.mqtt_port = 1883
        self.mqtt_topic = "your/topic"

        self.messages = []
        self.lock = threading.Lock()
        self.timer = None
        self.file_prefix = "raw_data"
        self.output_dir = Path(__file__).parent / "saved_data"
        self.output_dir.mkdir(exist_ok=True)
        self.client = mqtt.Client()

    def save_messages(self):
        """Saves the collected messages to a CSV file."""
        with self.lock:
            if not self.messages:
                return

            now = datetime.datetime.now()
            timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")
            filename = f"{self.file_prefix}_{timestamp}.csv"
            filepath = self.output_dir / filename
            messages_to_save = self.messages[:]
            self.messages = []

        try:
            with open(filepath, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["timestamp", "message"])
                writer.writerows(messages_to_save)
            print(f"Messages saved to {filepath}")
        except IOError as e:
            print(f"Error saving file: {e}")

    def schedule_save(self):
        """Schedules the save_messages function to run every 15 minutes."""
        self.save_messages()
        self.timer = threading.Timer(900, self.schedule_save)  # 900 seconds = 15 minutes
        self.timer.start()

    def on_connect(self, client, userdata, flags, rc):
        """The callback for when the client connects to the broker."""
        if rc == 0:
            print(f"Connected to MQTT Broker at {self.mqtt_broker}. Subscribing to topic '{self.mqtt_topic}'")
            client.subscribe(self.mqtt_topic)
            # Start the periodic save timer only after a successful connection
            print("Starting periodic saves. Press Ctrl+C to exit.")
            self.schedule_save()
        else:
            print(f"Failed to connect, return code {rc}\n")

    def on_message(self, client, userdata, msg):
        """The callback for when a PUBLISH message is received from the server."""
        timestamp = datetime.datetime.now().isoformat()
        message = msg.payload.decode('utf-8')
        
        # This logic is preserved from your original script to handle concatenated JSON
        corrected_message_string = message.replace('}{', '}\\n{')

        for json_str in corrected_message_string.splitlines():
            if json_str.strip():
                try:
                    parsed_json = json.loads(json_str)
                    print(f"Received: {json.dumps(parsed_json)}")
                    
                    with self.lock:
                        self.messages.append((timestamp, json_str))
                except json.JSONDecodeError:
                    print(f"Received (non-JSON): {json_str}")
                    with self.lock:
                        self.messages.append((timestamp, json_str))

    def run(self):
        """Connects to the MQTT broker and starts the message loop."""
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        try:
            self.client.connect(self.mqtt_broker, self.mqtt_port, 60)
            # loop_forever() is a blocking call that handles reconnecting.
            self.client.loop_forever()
        except Exception as e:
            print(f"An error occurred while connecting or in the loop: {e}")
        finally:
            print("Cleaning up...")
            if self.timer and self.timer.is_alive():
                self.timer.cancel()
                print("Cancelled periodic save timer.")
            self.save_messages() # Save any remaining messages
            self.client.disconnect()


if __name__ == "__main__":
    logger = MqttLogger()
    try:
        user_input = input("Enter a name for the data file (or press Enter for 'raw_data'): ").strip()
        if user_input:
            logger.file_prefix = user_input
        
        print(f"Files will be saved with prefix: {logger.file_prefix}")
        logger.run()
    except KeyboardInterrupt:
        print("\nProgram interrupted by user (Ctrl+C).")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        print("Exit.")

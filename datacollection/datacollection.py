import asyncio
import websockets
import csv
import datetime
import threading
import ssl
from pathlib import Path
import json

class WebSocketLogger:

    def __init__(self):
        self.messages = []
        self.lock = threading.Lock()
        self.timer = None
        self.file_prefix = "raw_data"
        self.output_dir = Path(__file__).parent / "testing_data"
        self.output_dir.mkdir(exist_ok=True)

    def save_messages(self):
        """Saves the collected messages to a CSV file."""
        with self.lock:
            if not self.messages:
                # This can be commented out if you don't want this message every 15 mins
                # print("No new messages to save.")
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

    async def run(self):
        """Connects to the websocket and listens for messages."""
        uri ="wss://10.233.45.165/ws"
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        try:
            async with websockets.connect(uri, ssl=ssl_context) as websocket:
                print(f"Connected to {uri}. Starting periodic saves. Press Ctrl+C to exit.")
                self.schedule_save()

                while True:
                    try:
                        message = await websocket.recv()
                        timestamp = datetime.datetime.now().isoformat()
                        
                        # --- MODIFICATION START ---
                        # Decode if the message is in bytes
                        if isinstance(message, bytes):
                            print(message)
                            message = message.decode('utf-8')
                            

                        # Handle concatenated JSON by replacing '}{' with a newline between them
                        # This correctly separates each JSON object
                        corrected_message_string = message.replace('}{', '}\n{')

                        # Process each JSON string individually
                        for json_str in corrected_message_string.splitlines():
                            if json_str.strip():
                                try:
                                    # Parse and print each individual JSON object
                                    parsed_json = json.loads(json_str)
                                    # print(f"Received: {json.dumps(parsed_json)}")
                                    
                                    # Save the individual JSON string to the list for the CSV
                                    with self.lock:
                                        self.messages.append((timestamp, json_str))
                                except json.JSONDecodeError:
                                    # Fallback for any line that isn't perfect JSON
                                    # print(f"Received (non-JSON): {json_str}")
                                    with self.lock:
                                         self.messages.append((timestamp, json_str))
                        # --- MODIFICATION END ---

                    except websockets.exceptions.ConnectionClosed:
                        print("Connection closed by server.")
                        break
        finally:
            print("Cleaning up...")
            if self.timer and self.timer.is_alive():
                self.timer.cancel()
                print("Cancelled periodic save timer.")
            self.save_messages()

if __name__ == "__main__":
    logger = WebSocketLogger()
    try:
        user_input = input("Enter a name for the data file (or press Enter for 'raw_data'): ").strip()
        if user_input:
            logger.file_prefix = user_input
        
        print(f"Files will be saved with prefix: {logger.file_prefix}")
        asyncio.run(logger.run())
    except KeyboardInterrupt:
        print("\nProgram interrupted by user (Ctrl+C).")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        print("Exit.")

import pandas as pd
import json
import io
from pathlib import Path

# This is sample data to make the script runnable.
# To use with your file, uncomment the line below and replace 'your_file.csv'
df = pd.read_csv('Scenario_1_data\\One_hour_2026-03-28_13-04-49.csv', sep=',')

# csv_data = """timestamp\tmessage
# 2026-03-28T12:49:57.378679\t{"component":"RG","data":{"radio_control":{}},"eventNum":372,"timestamp":"2026-03-28T07:19:57.878+0000","type":"heartbeat"}
# 2026-03-28T12:50:07.385379\t{"component":"RG","data":{"radio_control":{}},"eventNum":373,"timestamp":"2026-03-28T07:20:07.885+0000","type":"heartbeat"}
# 2026-03-28T12:50:12.673468\t{"data":{"antenna":1,"channel":866.3,"eventNum":16511,"format":"epc","idHex":"e28011700000020d2a412182","peakRssi":-60,"phase":-170.4800567626953,"reads":1},"timestamp":"2026-03-28T07:20:13.028+0000","type":"CUSTOM"}
# 2026-03-28T12:50:12.673468\t{"data":{"antenna":1,"channel":866.3,"eventNum":16512,"format":"epc","idHex":"e28011700000020d2a40c095","peakRssi":-61,"phase":-102.74727630615234,"reads":1},"timestamp":"2026-03-28T07:20:13.069+0000","type":"CUSTOM"}
# 2026-03-28T12:50:12.673468\t{"data":{"antenna":1,"channel":866.3,"eventNum":16513,"format":"epc","idHex":"e28011700000020d2a412b0c","peakRssi":-63,"phase":17.397380828857422,"reads":1},"timestamp":"2026-03-28T07:20:13.069+0000","type":"CUSTOM"}
# """
# df = pd.read_csv(io.StringIO(csv_data), sep='\t')

processed_data = []

# Iterate over each row in the DataFrame
for index, row in df.iterrows():
    try:
        # 1. Read the string from the 'message' column
        message_string = row['message']
        
        # 2. Convert the string into a usable JSON object
        msg = json.loads(message_string)
        
        # 3. Process the JSON object
        if 'data' in msg and isinstance(msg['data'], dict):
            # This check filters for messages that have a simple key-value structure in the "data" field
            if all(not isinstance(v, (dict, list)) for v in msg['data'].values()):
                 record = {'timestamp': row['timestamp']}
                 record.update(msg['data'])
                 processed_data.append(record)

    except (json.JSONDecodeError, TypeError):
        # This will catch any rows where the 'message' is not a valid JSON string
        print(f"Warning: Could not decode JSON for row {index}")

# Create the final DataFrame from our processed list
result_df = pd.DataFrame(processed_data)

output_path = Path('saved_data') / 'result_df.csv'
output_path.parent.mkdir(parents=True, exist_ok=True)
result_df.to_csv(output_path, index=False)

print("--- Processed Data ---")
print(result_df)
print(f"Saved processed data to: {output_path}")

import json
import csv
from collections import Counter

# Input and output file paths (use raw strings or escape backslashes for file paths)
input_file_path = r"C:\course\RIT\CADETS_E3\CADETS_E3\json_to_csv\to_csv.json"
output_file_path = r"C:\course\RIT\CADETS_E3\CADETS_E3\json_to_csv\first_draft.csv"

# Create counters to track all unique event type and file object type counts
event_type_counter = Counter()
file_object_type_counter = Counter()

# Open the input and output files
with open(input_file_path, "r") as json_file, open(output_file_path, "w", newline='') as csv_file:
    # Create a CSV writer
    csv_writer = None
    
    # Process each line (entry) in the JSON file
    for line in json_file:
        # Skip empty lines
        if not line.strip():
            continue
        
        # Parse the JSON line into a dictionary
        data = json.loads(line)
        
        # Flatten the necessary parts from your JSON structure for both Event and FileObject
        event = data.get("datum", {}).get("com.bbn.tc.schema.avro.cdm18.Event", {})
        file_object = data.get("datum", {}).get("com.bbn.tc.schema.avro.cdm18.FileObject", {})
        
        # Extract the event type and file object type
        event_type = event.get("type")
        file_object_type = file_object.get("type")
        
        # Increment the counter for the event type (if available)
        if event_type:
            event_type_counter[event_type] += 1
        
        # Increment the counter for the file object type (if available)
        if file_object_type:
            file_object_type_counter[file_object_type] += 1
        
        # Flatten the data for both Event and FileObject
        flattened_data = {
            # Event fields
            "event_uuid": event.get("uuid"),
            "event_sequence": event.get("sequence", {}).get("long"),
            "event_type": event_type,  # Add event type to the output CSV
            "event_threadId": event.get("threadId", {}).get("int"),
            "event_hostId": event.get("hostId"),
            "event_timestampNanos": event.get("timestampNanos"),
            "event_name": event.get("name", {}).get("string"),
            "event_parameters": str(event.get("parameters", {}).get("array", [])),  # Convert list to string
            "event_properties": str(event.get("properties", {}).get("map", {})),  # Convert dict to string
            
            # FileObject fields
            "file_object_uuid": file_object.get("uuid"),
            "file_object_sequence": file_object.get("sequence", {}).get("long"),
            "file_object_type": file_object_type,  # Add file object type to the output CSV
            "file_object_threadId": file_object.get("threadId", {}).get("int"),
            "file_object_hostId": file_object.get("hostId"),
            "file_object_timestampNanos": file_object.get("timestampNanos"),
            "file_object_name": file_object.get("name", {}).get("string"),
            "file_object_parameters": str(file_object.get("parameters", {}).get("array", [])),  # Convert list to string
            "file_object_properties": str(file_object.get("properties", {}).get("map", {})),  # Convert dict to string
        }
        
        # Initialize the CSV writer with headers on the first iteration
        if csv_writer is None:
            # Extract keys for CSV header from flattened data
            headers = flattened_data.keys()
            csv_writer = csv.DictWriter(csv_file, fieldnames=headers)
            csv_writer.writeheader()

        # Write the row to the CSV file
        csv_writer.writerow(flattened_data)

# After processing, print the counts of all event types and file object types
print("Count of all event types:")
for event_type, count in event_type_counter.items():
    print(f"{event_type}: {count}")

print("\nCount of all file object types:")
for file_object_type, count in file_object_type_counter.items():
    print(f"{file_object_type}: {count}")

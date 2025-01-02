import os
import json
import time
from dotenv import load_dotenv
import requests

load_dotenv()
with open(os.environ['BUILD2_TOKEN']) as f:
    bearer_token = f.read()


with open(os.environ['RESOURCE_ALIAS_MAP']) as f:
    resource_alias_map = json.load(f)

input_info = resource_alias_map['raw-data-01']
output_info = resource_alias_map['processed-supplier-data']

input_rid = input_info['rid']
input_branch = input_info['branch'] or "master"
output_rid = output_info['rid']
output_branch = output_info['branch'] or "master"


FOUNDRY_URL = os.getenv('FOUNDRY_URL')
FOUNDRY_TOKEN = os.getenv('FOUNDRY_TOKEN')
POST_URI = os.getenv('POST_URI')

# 1. DATA --> COMPUTE MODULE
def load_data_from_file(filename):

    try:
        # Open and read the JSON file
        with open(filename, "r") as json_file:
            data = json.load(json_file)  # Load the content of the file into a Python dictionary
            return data

    except FileNotFoundError:
        print(f"The file {filename} was not found.")
        raise
    except json.JSONDecodeError:
        print(f"Error decoding JSON from the file {filename}.")
        raise

# 1.1 Process the Data
def process_data(data):

    # Ensure the 'results' field is in the correct format (array of rows)
    if "results" in data and isinstance(data["results"], list):
        rows = data["results"]
    else:
        rows = []
    return rows

# 2. COMPUTE MODULE --> INPUT STREAM
def push_suppliers_to_input(token, rows):
        
    # Ensure the token is valid
    if not token:
        raise ValueError("Authorization token is missing!")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    # Format the body payload
    body = json.dumps([{"value": row} for row in rows])

    try:
        response = requests.post(POST_URI, data=body, headers=headers)
        response.raise_for_status()  # Raise an error for bad status codes
        print("Data pushed successfully!")

    except requests.exceptions.RequestException as e:
        print(f"Error during request: {e}")
        if response is not None:
            try:
                error_data = response.json()
                print(f"Error details: {error_data.get('message', 'No message available')}")
            except json.JSONDecodeError:
                print("No JSON response received.")
        raise

# 4. INPUT STREAM --> COMPUTE MODULE
def get_stream_latest_records():
    url = f"https://{FOUNDRY_URL}/stream-proxy/api/streams/{input_rid}/branches/{input_branch}/records"
    response = requests.get(url, headers={"Authorization": f"Bearer {bearer_token}"})
    print("Data being fetched from the input stream")
    return response.json()

# 4.1 Changing the data type from string to array of string
def process_record(record):
    updated_record = {
        "provider_id": record["value"].get("provider_id", ""),
        "acceptsassignement": record["value"].get("acceptsassignement", ""),
        "participationbegindate": record["value"].get("participationbegindate", ""),
        "businessname": record["value"].get("businessname", ""),
        "practicename": record["value"].get("practicename", ""),
        "practiceaddress1": record["value"].get("practiceaddress1", ""),
        "practiceaddress2": record["value"].get("practiceaddress2", ""),
        "practicecity": record["value"].get("practicecity", ""),
        "practicestate": record["value"].get("practicestate", ""),
        "practicezip9code": record["value"].get("practicezip9code", ""),
        "telephonenumber": record["value"].get("telephonenumber", ""),
        "specialitieslist": record["value"].get("specialitieslist", "").split('|') if "specialitieslist" in record["value"] else [],
        "providertypelist": record["value"].get("providertypelist", "").split('|') if "providertypelist" in record["value"] else [],
        "supplieslist": record["value"].get("supplieslist", "").split('|') if "supplieslist" in record["value"] else [],
        "latitude": record["value"].get("latitude", ""),
        "longitude": record["value"].get("longitude", ""),
        "is_contracted_for_cba": record["value"].get("is_contracted_for_cba", ""),
    }
    return updated_record

# 5. COMPUTE MODULE --> OUTPUT STREAM
def put_record_to_stream(record):
    url = f"https://{FOUNDRY_URL}/stream-proxy/api/streams/{output_rid}/branches/{output_branch}/jsonRecord"
    print("Data being pushed to the output stream")
    requests.post(url, json=record, headers={"Authorization": f"Bearer {bearer_token}"})

def main():
    print("Starting the app")
    filename = 'data.json'  
    data = load_data_from_file(filename)
    suppliers = process_data(data)
    push_suppliers_to_input(FOUNDRY_TOKEN, suppliers)
    records = get_stream_latest_records()
    processed_records = list(map(process_record, records['records']))
    [put_record_to_stream(record) for record in processed_records]
    time.sleep(60)

if __name__ == "__main__":
    main()
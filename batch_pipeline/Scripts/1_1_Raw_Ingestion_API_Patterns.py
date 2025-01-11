import requests, json, csv, io
import warnings
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError

# GCP Configuration
project_id = "data-eng-dev-437916"
bucket_name = "edit-data-eng-project-group1"

def fetch_trips_data():
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob("LandingZone/GTFS/trips.txt")

    pattern_ids = set()
    content = blob.download_as_text()
    csv_reader = csv.DictReader(io.StringIO(content))
    for row in csv_reader:
        if 'pattern_id' in row:
            pattern_ids.add(row['pattern_id'])

    print(f"Fetched {len(pattern_ids)} distinct pattern IDs from trips.txt")
    return list(pattern_ids)

def fetch_and_append_patterns_data(pattern_ids):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob("LandingZone/carris_patterns_data.json")

    for pattern_id in pattern_ids:
        url = f"https://api.carrismetropolitana.pt/patterns/{pattern_id}"
        response = requests.get(url)
        
        if response.status_code == 200:
            pattern_data = response.json()
            
            # Append the new pattern data to the blob
            current_content = blob.download_as_text() if blob.exists() else ""
            updated_content = current_content + json.dumps(pattern_data) + '\n'
            blob.upload_from_string(updated_content)
            
            print(f"Fetched and appended pattern data for ID {pattern_id}")
        else:
            print(f"Failed to fetch pattern data for ID {pattern_id}. Status code: {response.status_code}")

def main():
    try:
        pattern_ids = fetch_trips_data()
        warnings.filterwarnings("ignore", message="Your application has authenticated using end user credentials") # Ignore the specific UserWarning
        fetch_and_append_patterns_data(pattern_ids)
        print(f"All pattern data has been written to the GCP bucket")

    except GoogleCloudError as e:
        print(f"A Google Cloud error occurred: {str(e)}")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    main()

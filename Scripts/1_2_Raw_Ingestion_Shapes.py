
import requests, json, csv, io, warnings
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError

# GCP Configuration
project_id = "data-eng-dev-437916"
bucket_name = "edit-data-eng-project-group1"

def fetch_trips_data():
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob("LandingZone/GTFS/trips.txt")

    shape_ids = set()
    content = blob.download_as_text()
    csv_reader = csv.DictReader(io.StringIO(content))
    for row in csv_reader:
        if 'shape_id' in row:
            shape_ids.add(row['shape_id'])

    print(f"Fetched {len(shape_ids)} distinct shape IDs from trips.txt")
    return list(shape_ids)

def fetch_and_append_shapes_data(shape_ids):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob("LandingZone/carris_shapes_data.json")

    for shape_id in shape_ids:
        url = f"https://api.carrismetropolitana.pt/shapes/{shape_id}"
        response = requests.get(url)
        
        if response.status_code == 200:
            shape_data = response.json()
            
            # Append the new shape data to the blob
            current_content = blob.download_as_text() if blob.exists() else ""
            updated_content = current_content + json.dumps(shape_data) + '\n'
            blob.upload_from_string(updated_content)
            
            print(f"Fetched and appended shape data for ID {shape_id}")
        else:
            print(f"Failed to fetch shape data for ID {shape_id}. Status code: {response.status_code}")

def main():
    try:
        shape_ids = fetch_trips_data()
        warnings.filterwarnings("ignore", message="Your application has authenticated using end user credentials") # Ignore the specific UserWarning
        fetch_and_append_shapes_data(shape_ids)
        print(f"All shapes data has been written to the GCP bucket")

    except GoogleCloudError as e:
        print(f"A Google Cloud error occurred: {str(e)}")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    main()

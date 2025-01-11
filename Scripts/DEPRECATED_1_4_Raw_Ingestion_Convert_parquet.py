# Packages to Install:
# pandas, pyarrow and fastparquet

from google.cloud import storage
import pandas as pd
import io
import os
import json
import warnings

# GCP Configuration
project_id = "data-eng-dev-437916"
bucket_name = "edit-data-eng-project-group1"
landing_zone_folder = "LandingZone"
converted_folder = "LandingZone_converted"

def list_files_in_folder(bucket, folder_name):
    blobs = bucket.list_blobs(prefix=folder_name)
    return [blob for blob in blobs if not blob.name.endswith('/')]

def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def convert_to_parquet(blob):
    file_content = blob.download_as_text()
    file_extension = os.path.splitext(blob.name)[1]

    if file_extension == '.json':
        try:
            # Try to parse the entire content as a single JSON object
            data = json.loads(file_content)
            flattened_data = flatten_json(data)
            df = pd.DataFrame([flattened_data])
        except json.JSONDecodeError:
            # If that fails, try to parse each line as a separate JSON object
            json_data = []
            for line in file_content.strip().split('\n'):
                try:
                    data = json.loads(line)
                    flattened_data = flatten_json(data)
                    json_data.append(flattened_data)
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON line: {e}")
                    continue
            df = pd.DataFrame(json_data)
    elif file_extension == '.txt':
        df = pd.read_csv(io.StringIO(file_content), sep='\t')
    else:
        return None

    if df.empty:
        print(f"No valid data found in {blob.name}")
        return None

    # Replace dots with underscores in column names
    df.columns = df.columns.str.replace('.', '_')

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    return parquet_buffer

def main():
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    files = list_files_in_folder(bucket, landing_zone_folder)

    for file in files:
        if file.name.endswith(('.txt', '.json')):
            print(f"Converting {file.name} to parquet")
            warnings.filterwarnings("ignore", message="Your application has authenticated using end user credentials")
            parquet_data = convert_to_parquet(file)
            
            if parquet_data:
                warnings.filterwarnings("ignore", message="Your application has authenticated using end user credentials")
                new_blob_name = os.path.join(converted_folder, os.path.splitext(os.path.basename(file.name))[0] + '.parquet')
                new_blob = bucket.blob(new_blob_name)
                new_blob.upload_from_file(parquet_data, content_type='application/octet-stream')
                print(f"Uploaded {new_blob_name}")
            else:
                print(f"Skipped {file.name} - not a valid .txt or .json file")

if __name__ == "__main__":
    main()

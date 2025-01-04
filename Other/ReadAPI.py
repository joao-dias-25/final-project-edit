# Create virtual environment: python3 -m venv venv
# Activate venv: source .venv/bin/activate
# Execute script in WSl using python ReadAPI.py


import requests, zipfile, io, json, os, csv


# Get GTFS zip
def fetch_gtfs_data(path):
    url = "https://api.carrismetropolitana.pt/gtfs"
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        z = zipfile.ZipFile(io.BytesIO(response.content)) # Use io.BytesIO for in-memory byte streams
        return z.extractall(path)
        print(f"GTFS files saved to {path}.")
    else:
        raise Exception(f"API request failed with status code {response.status_code}")
    
    
# Get ALERTS json
def fetch_alerts_data():
    url = "https://api.carrismetropolitana.pt/alerts"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API request failed with status code {response.status_code}")

# Get MUNICIPALITIES json
def fetch_municipalities_data():
    url = "https://api.carrismetropolitana.pt/municipalities"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API request failed with status code {response.status_code}")

# Get STOPS json
def fetch_stops_data():
    url = "https://api.carrismetropolitana.pt/stops"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API request failed with status code {response.status_code}")

# Get LINES json
def fetch_lines_data():
    url = "https://api.carrismetropolitana.pt/lines"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API request failed with status code {response.status_code}")

# Get ROUTES json
def fetch_routes_data():
    url = "https://api.carrismetropolitana.pt/routes"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API request failed with status code {response.status_code}")

# Get ENCM json
def fetch_encm_data():
    url = "https://api.carrismetropolitana.pt/datasets/facilities/encm"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API request failed with status code {response.status_code}")

# Get SCHOOLS json
def fetch_schools_data():
    url = "https://api.carrismetropolitana.pt/datasets/facilities/schools"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API request failed with status code {response.status_code}")

# Aux to read from the trips.txt for pattern_id and shape_id
def fetch_trips_data():
    trips_data = []
    trips_file_path = os.path.join("./LandingZone/GTFS", "trips.txt")
    
    if not os.path.exists(trips_file_path):
        raise FileNotFoundError(f"trips.txt file not found at {trips_file_path}")
    
    pattern_ids = set()
    with open(trips_file_path, 'r') as trips_file:
        csv_reader = csv.DictReader(trips_file)
        for row in csv_reader:
            if 'pattern_id' in row:
                pattern_ids.add(row['pattern_id'])
    
    print(f"Fetched {len(pattern_ids)} distinct pattern IDs from trips.txt")
    return list(pattern_ids)

# Get PATTERNS json
#def fetch_patterns_data(pattern_ids):
#    patterns_data = []
#    for pattern_id in pattern_ids:
#        url = f"https://api.carrismetropolitana.pt/patterns/{pattern_id}"
#        response = requests.get(url)
#        
#        if response.status_code == 200:
#            patterns_data.append(response.json())
#            print(f"Fetched pattern data for ID {pattern_id}")
#        else:
#            print(f"Failed to fetch pattern data for ID {pattern_id}. Status code: {response.status_code}")
#    
#    return patterns_data

def fetch_and_append_patterns_data(pattern_ids, output_file):
    for pattern_id in pattern_ids:
        url = f"https://api.carrismetropolitana.pt/patterns/{pattern_id}"
        response = requests.get(url)
        
        if response.status_code == 200:
            pattern_data = response.json()
            
            # Append the new pattern data to the file
            with open(output_file, 'a') as f:
                json.dump(pattern_data, f)
                f.write('\n')  # Add a newline for readability
            
            print(f"Fetched and appended pattern data for ID {pattern_id}")
        else:
            print(f"Failed to fetch pattern data for ID {pattern_id}. Status code: {response.status_code}")


## Function to write API response as JSON file in specified path
def save_to_local(data, folder_path, file_name):
    # Ensure the folder exists
    os.makedirs(folder_path, exist_ok=True)

    # Define the full file path
    file_path = os.path.join(folder_path, file_name)

    # Save the data to the file
    with open(file_path, 'w') as json_file:
        json.dump(data, json_file, indent=2)

    print(f"File {file_name} saved to {folder_path}.")



def main():
    try:
        # Fetch data from the API       
        alerts_data = fetch_alerts_data()
        municipalities_data = fetch_municipalities_data()
        stops_data = fetch_stops_data()
        lines_data = fetch_lines_data()
        routes_data = fetch_routes_data()
        encm_data = fetch_encm_data()
        schools_data = fetch_schools_data()
        

        # Save data to local folder
        folder_path = "./LandingZone"
        gtfs_folder_path = "./LandingZone/GTFS"

        #GTFS
        gtfs_data = fetch_gtfs_data(gtfs_folder_path)

        #ALERTS
        file_name_alerts = "carris_alerts_data.json"
        save_to_local(alerts_data, folder_path, file_name_alerts)
        #MUNICIPALITIES
        file_name_municipalities = "carris_municipalities_data.json"
        save_to_local(municipalities_data, folder_path, file_name_municipalities)
        #STOPS
        file_name_stops = "carris_stops_data.json"
        save_to_local(stops_data, folder_path, file_name_stops)
        #LINES
        file_name_lines = "carris_lines_data.json"
        save_to_local(lines_data, folder_path, file_name_lines)
        #ROUTES
        file_name_routes = "carris_routes_data.json"
        save_to_local(routes_data, folder_path, file_name_routes)
        #ENCM
        file_name_encm = "carris_encm_data.json"
        save_to_local(encm_data, folder_path, file_name_encm)
        #SCHOOLS
        file_name_encm = "carris_encm_data.json"
        save_to_local(encm_data, folder_path, file_name_encm)


        ## Fetch distinct pattern IDs from trips.txt
        #pattern_ids = fetch_trips_data()
        ## Fetch patterns data using the distinct pattern IDs
        #patterns_data = fetch_patterns_data(pattern_ids)
        ## Save patterns data
        #save_to_local(patterns_data, "./LandingZone", "carris_patterns_data.json")

        # Fetch distinct pattern IDs from trips.txt
        pattern_ids = fetch_trips_data()
        # Define the output file path
        output_file = "./LandingZone/carris_patterns_data.json"
        # Clear the file if it exists
        open(output_file, 'w').close()
        # Fetch patterns data and append to file
        fetch_and_append_patterns_data(pattern_ids, output_file)
        print(f"All pattern data has been written to {output_file}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()

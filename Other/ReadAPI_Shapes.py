

import requests, zipfile, io, json, os, csv


# Aux to read from the trips.txt for shape_id and shape_id
def fetch_trips_data():
    trips_data = []
    trips_file_path = os.path.join("./LandingZone/GTFS", "trips.txt")
    
    if not os.path.exists(trips_file_path):
        raise FileNotFoundError(f"trips.txt file not found at {trips_file_path}")
    
    shape_ids = set()
    with open(trips_file_path, 'r') as trips_file:
        csv_reader = csv.DictReader(trips_file)
        for row in csv_reader:
            if 'shape_id' in row:
                shape_ids.add(row['shape_id'])
    
    print(f"Fetched {len(shape_ids)} distinct shape IDs from trips.txt")
    return list(shape_ids)

# Get PATTERNS json
#def fetch_patterns_data(shape_ids):
#    shapes_data = []
#    for shape_id in shape_ids:
#        url = f"https://api.carrismetropolitana.pt/shapes/{shape_id}"
#        response = requests.get(url)
#        
#        if response.status_code == 200:
#            patterns_data.append(response.json())
#            print(f"Fetched shape data for ID {shape_id}")
#        else:
#            print(f"Failed to fetch shape data for ID {shape_id}. Status code: {response.status_code}")
#    
#    return patterns_data

def fetch_and_append_shapes_data(shape_ids, output_file):
    for shape_id in shape_ids:
        url = f"https://api.carrismetropolitana.pt/shapes/{shape_id}"
        response = requests.get(url)
        
        if response.status_code == 200:
            pattern_data = response.json()
            
            # Append the new pattern data to the file
            with open(output_file, 'a') as f:
                json.dump(pattern_data, f)
                f.write('\n')  # Add a newline for readability
            
            print(f"Fetched and appended shape data for ID {shape_id}")
        else:
            print(f"Failed to fetch shape data for ID {shape_id}. Status code: {response.status_code}")


def main():
    try:

         ## Fetch distinct pattern IDs from trips.txt
        #shape_ids = fetch_trips_data()
        ## Fetch patterns data using the distinct pattern IDs
        #patterns_data = fetch_patterns_data(shape_ids)
        ## Save patterns data
        #save_to_local(patterns_data, "./LandingZone", "carris_patterns_data.json")

        # Fetch distinct pattern IDs from trips.txt
        shape_ids = fetch_trips_data()
        # Define the output file path
        output_file = "./LandingZone/carris_shapes_data.json"
        # Clear the file if it exists
        open(output_file, 'w').close()
        # Fetch patterns data and append to file
        fetch_and_append_shapes_data(shape_ids, output_file)
        print(f"All shapes data has been written to {output_file}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
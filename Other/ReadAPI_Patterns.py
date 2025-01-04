

import requests, zipfile, io, json, os, csv


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


def main():
    try:

         ## Fetch distinct pattern IDs from trips.txt
        #pattern_ids = fetch_trips_data()
        ## Fetch patterns data using the distinct pattern IDs
        #patterns_data = fetch_patterns_data(pattern_ids)
        ## Save patterns data
        #save_to_local(patterns_data, "./LandingZone", "carris_patterns_data.json")

        # Fetch distinct pattern IDs from trips.txt
        pattern_ids = fetch_trips_data()
        # Define the output file path
        output_file = "./LandingZone/carris_patterns_data_new.json"
        # Clear the file if it exists
        open(output_file, 'w').close()
        # Fetch patterns data and append to file
        fetch_and_append_patterns_data(pattern_ids, output_file)
        print(f"All pattern data has been written to {output_file}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()


import requests
import sys

URI = f"http://localhost:8000/get_data"

def fetch_data(scid, metric):
    #Fetch Data from API
    response = requests.get(URI, params={"scid": scid, "metric": metric})
    if response.status_code == 200:
        data = response.json()
        return len(data['data'])
    
    else:
         print(f"Error accessing items endpoint: {response.status_code}, Response: {response.text}")
         return {}
def test_root_endpoint(scid, metric):
    response = requests.get(f"http://localhost:8000/get_data", params={"scid": scid, "metric": metric})
    if response.status_code == 200:
        X = response.json()
        print(len(X['data']))
        #print("Root Endpoint Response:", len(response.json()))
    else:
         print(f"Error accessing items endpoint: {response.status_code}, Response: {response.text}")


if __name__ == "__main__":
    # Test the root endpoint
    scid = sys.argv[1]
    metric = sys.argv[2]
    test_root_endpoint(scid, metric)
  
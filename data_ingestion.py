from apilinks import API_LINKS
import requests 

def fetch_data_from_api(api_link):
    try:
        response = requests.get(api_link)
        response.raise_for_status() 
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {api_link}: {e}")
        return None

def main():
    all_data = []
    for api_link in API_LINKS:
        data = fetch_data_from_api(api_link)
        if data is not None:
            all_data.extend(data)
    
    print(f"Total records fetched: {len(all_data)}")
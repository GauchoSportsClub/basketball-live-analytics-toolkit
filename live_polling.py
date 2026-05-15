import requests
import pandas as pd

def fetch_live_espn_data(api_url):
    """Fetches a single snapshot of live data to be called on an interval by the frontend."""
    try:
        response = requests.get(api_url)
        if response.status_code == 200:
            data = response.json()
            return process_live_feed(data)
    except Exception as e:
        print(f"Error fetching live data: {e}")
    return None

def process_live_feed(raw_data):
    df = pd.DataFrame(raw_data.get('events', []))
    return df

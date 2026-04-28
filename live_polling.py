import requests
import time
import pandas as pd

def start_live_polling(api_url, poll_rate):
    while True:
        try:
            response = requests.get(api_url)
            if response.status_code == 200:
                data = response.json()
                process_live_feed(data)
        except Exception as e:
            print(e)
        time.sleep(poll_rate)

def process_live_feed(raw_data):
    df = pd.DataFrame(raw_data.get('events', []))
    return df

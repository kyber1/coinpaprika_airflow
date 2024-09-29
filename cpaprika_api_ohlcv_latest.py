import requests
import json
import time
import os
from datetime import datetime, timezone

#free url
base_url = "https://api.coinpaprika.com/v1"

def get_ohlcv_latest_data(coin_id):
    full_url = f"{base_url}/coins/{coin_id}/ohlcv/latest"
    response = requests.get(full_url)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching OHLCV data for {coin_id}: {response.status_code}")
        return None

if __name__ == "__main__":
    # Read api_ids from api_ids.txt
    with open('api_ids.txt', 'r') as f:
        api_ids = [line.strip() for line in f if line.strip()]
    
    all_coins_data = {}

    for coin_id in api_ids:
        ohlcv_data_for_coin = get_ohlcv_latest_data(coin_id)
        
        if ohlcv_data_for_coin:
            all_coins_data[coin_id] = ohlcv_data_for_coin[0]
            print(f"Fetched OHLCV data for {coin_id}")
        else:
            print(f"Failed to get OHLCV data for {coin_id}")
        
        time.sleep(1)
    
    # Get current date components using timezone-aware datetime
    today = datetime.now(timezone.utc)
    year = today.strftime('%Y')
    month = today.strftime('%m')
    day = today.strftime('%d')

    dir_path = os.path.join('data', year, month, day)
    os.makedirs(dir_path, exist_ok=True)

    # Save all data to a single JSON file
    output_filename = os.path.join(dir_path, 'all_ohlcv_latest.json')
    with open(output_filename, 'w') as outfile:
        json.dump(all_coins_data, outfile, indent=4)
    print(f"Saved all OHLCV data to {output_filename}")
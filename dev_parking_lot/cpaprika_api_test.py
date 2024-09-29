import requests

#free url
base_url = "https://api.coinpaprika.com/v1"

#fetch list coins api endpoint
def get_list_coins():
    full_url = f"{base_url}/coins"
    response = requests.get(full_url)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error while getting list of coins: {response.status_code}")
        return None

def filter_coins_by_blockhain(coins_data, blockhain_targets):
    filtered_coins = {name: [] for name in blockhain_targets}

    for coin in coins_data:
        blockhain_id = coin['id'].split('-')[0]
    
        if blockhain_id in blockhain_targets and coin['is_active']:
            coin_info = {
                'id': coin['id'],
                'name': coin['name'],
                'symbol': coin['symbol'],
                'rank': coin['rank'],
                'type': coin['type']
            }
            filtered_coins[blockhain_id].append(coin_info)
    
    return filtered_coins

if __name__ == "__main__":
    coins_data = get_list_coins()
    blockhain_targets = ['btc', 'eth', 'sol']

    if coins_data:
        filtered_coins = filter_coins_by_blockhain(coins_data, blockhain_targets)

        for blockchain, coins in filtered_coins.items():
            print(f"Coins for blockchain '{blockchain}':")
            for coin in coins:
                print(f"  ID: {coin['id']}, Name: {coin['name']}, Symbol: {coin['symbol']}, "
                      f"Rank: {coin['rank']}, Type: {coin['type']}")
    else:
        print("No coin data available.")
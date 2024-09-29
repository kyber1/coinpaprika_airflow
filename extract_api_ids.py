import json

with open('top100coins.json', 'r') as f:
    data = json.load(f)

api_ids = [item['api_id'] for item in data['data']]

with open('api_ids.txt', 'w') as f:
    for api_id in api_ids:
        f.write(f"{api_id}\n")
import requests

url = "https://api-frontend.coinpaprika.com/ajax/coins/1?section=home-overview&all=false&expand=home-overview%2Cprice_stats&sort%5Bsort%5D=index&sort%5Bsortorder%5D=asc&tagID=&filters%5Bcoins%5D=true&filters%5Btokens%5D=true&currency=usd"

response = requests.get(url)

print(response)
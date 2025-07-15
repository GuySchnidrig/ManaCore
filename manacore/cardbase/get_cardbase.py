import requests
import pandas as pd

# Define your 5 cube names for each season
cube_seasons = {
    'Season-1': '6092e946b604dd10525e1151',
    'Season-2': '62cc20a037029269f33e0fb1',
    'Season-3': 'ba6a8266-80da-4230-9913-5f070f3c154c',
    'Season-4': '42e778b4-454c-45ff-9001-45285b519068',
    'Season-5': '41113702-0533-4a3b-a52d-cbc14a32ed61',
}

all_data = []

for season, cube_name in cube_seasons.items():
    url = f"https://cubecobra.com/cube/api/cubeJSON/{cube_name}"
    resp = requests.get(url)
    resp.raise_for_status()
    cube_json = resp.json()
    cards = cube_json['cards']['mainboard']

    for card in cards:
        card_id = card.get('cardID')
        tags = card.get('tags', [])
        colors = card.get('colors', [])
        
        all_data.append({
            'season': season,
            'cardID': card_id,
            'tags': tags,
            'colors': colors
        })

df = pd.DataFrame(all_data)
print(df)

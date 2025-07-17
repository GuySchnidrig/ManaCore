import os
import json
import requests
import pandas as pd
from datetime import datetime, timezone



def fetch_cube_mainboard(season_data, season_start_dates):
    """Fetch cube mainboard cards with timestamp for each season."""
    all_data = []

    for season, data in season_data.items():
        cube_id = data['cube_id']
        url = f"https://cubecobra.com/cube/api/cubeJSON/{cube_id}"
        resp = requests.get(url)
        resp.raise_for_status()
        cube_json = resp.json()
        cards = cube_json['cards']['mainboard']

        timestamp = season_start_dates.get(season, '1970-01-01')

        for card in cards:
            all_data.append({
                'season': season,
                'timestamp': timestamp,
                'scryfallId': card.get('cardID'),
                'tags': card.get('tags', [])
            })
    return all_data


def export_mainboard_csv(mainboard_data, output_csv='data/processed/cube_mainboard.csv'):
    """Export mainboard snapshot data as CSV."""
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)

    df = pd.DataFrame(mainboard_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values(['season', 'timestamp', 'scryfallId'])
    df.to_csv(output_csv, index=False)
    print(f"Mainboard snapshot CSV saved: {output_csv}")

def fetch_all_cube_histories(season_data, output_dir='data/processed/history'):
    """
    Fetch and save history for all cubes in the provided season mapping.
    Saves each cube's history to a separate JSON file.
    """
    os.makedirs(output_dir, exist_ok=True)

    for season, data in season_data.items():
        cube_id = data["cube_id"]
        url = f'https://cubecobra.com/public/cube/history/{cube_id}'
        response = requests.post(url, json={}, headers={'Content-Type': 'application/json'})

        if response.ok:
            history_data = response.json()
            output_path = os.path.join(output_dir, f'{season}_history.json')

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(history_data, f, indent=2, ensure_ascii=False)

            print(f"✅ Saved history for {season} to {output_path}")
        else:
            print(f"❌ Failed to fetch history for {season}: {response.status_code}")



def load_combined_history_data(history_dir='data/processed/history'):
    """
    Load and return combined 'posts' with season info from multiple cube history JSON files in a directory.
    Returns a list of tuples: (season, post).
    """

    all_posts_with_season = []

    for filename in sorted(os.listdir(history_dir)):
        if not filename.endswith('_history.json'):
            continue

        path = os.path.join(history_dir, filename)
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if "posts" not in data or not isinstance(data["posts"], list):
            raise ValueError(f"Expected 'posts' list in {filename}")

        # Extract season from filename, e.g., "Season-1_history.json" -> "Season-1"
        season = filename.replace('_history.json', '')

        for post in data["posts"]:
            all_posts_with_season.append((season, post))

    print(f"✅ Loaded {len(all_posts_with_season)} history entries from {history_dir}")
    return all_posts_with_season


def process_history_entries(posts_with_season):
    """Process a list of (season, CubeCobra history post) tuples."""
    history_records = []

    for season, entry in posts_with_season:
        timestamp_ms = entry.get("date")
        if not timestamp_ms:
            continue

        timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).date().isoformat()

        changelog = entry.get("changelog", {})
        mainboard_changes = changelog.get("mainboard", {})

        for change_type in ["adds", "removes"]:
            changes = mainboard_changes.get(change_type, [])
            for card_entry in changes:
                # Determine the card object based on change type
                card = (
                    card_entry.get("oldCard") if change_type == "removes"
                    else card_entry.get("newCard", card_entry)
                )

                if not isinstance(card, dict) or "cardID" not in card:
                    continue

                history_records.append({
                    "season": season,
                    "timestamp": timestamp,
                    "change_type": change_type,
                    "scryfallId": card["cardID"]
                })

    return history_records




def export_history_csv(history_data, output_csv='data/processed/cube_history.csv'):
    """Export cleaned history change data with incremental IDs."""
    import pandas as pd
    import os

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)

    df = pd.DataFrame(history_data)

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp').reset_index(drop=True)


    df.insert(0, 'change_id', range(1, len(df) + 1))

    df.to_csv(output_csv, index=False)
    print(f"History changes CSV saved: {output_csv}")


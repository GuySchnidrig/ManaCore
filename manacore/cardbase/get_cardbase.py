import os
import json
import requests
import pandas as pd
from datetime import datetime, timezone



def fetch_cube_mainboard(season_data, season_start_dates):
    """
    Fetch mainboard cube card data for each season with associated timestamps.

    Parameters:
    -----------
    season_data : dict
        Dictionary containing season metadata, including 'cube_id' for each season.
    season_start_dates : dict
        Dictionary mapping season IDs to their start dates (ISO format strings).

    Returns:
    --------
    list of dict
        A list where each element is a dictionary representing a card entry with keys:
        - 'season_id': The season identifier.
        - 'timestamp': The start date of the season (ISO format string).
        - 'scryfallId': The card's unique ID from Scryfall.
        - 'tags': A list of tags associated with the card (empty list if none).
    """
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
                'season_id': season,
                'timestamp': timestamp,
                'scryfallId': card.get('cardID'),
                'tags': card.get('tags', [])
            })
    return all_data


def export_mainboard_csv(mainboard_data, output_csv='data/processed/cube_mainboard.csv'):
    """
    Export mainboard card snapshot data to a CSV file.

    Parameters:
    -----------
    mainboard_data : list of dict
        List of dictionaries containing mainboard card data, each with keys:
        - 'season_id'
        - 'timestamp' (ISO date string)
        - 'scryfallId'
        - 'tags'
    
    output_csv : str, optional
        File path to save the CSV output (default is 'data/processed/cube_mainboard.csv').

    Behavior:
    ---------
    - Ensures the output directory exists.
    - Converts the data into a pandas DataFrame.
    - Parses 'timestamp' column as datetime.
    - Sorts data by 'season_id', 'timestamp', and 'scryfallId'.
    - Writes the sorted data to the specified CSV file without the index.
    - Prints a confirmation message after saving.
    """
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)

    df = pd.DataFrame(mainboard_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values(['season_id', 'timestamp', 'scryfallId'])
    df.to_csv(output_csv, index=False)
    print(f"Mainboard snapshot CSV saved: {output_csv}")

def fetch_all_cube_histories(season_data, output_dir='data/processed/history'):
    """
    Fetch and save history data for all cubes defined in the season mapping.

    Parameters:
    -----------
    season_data : dict
        Dictionary mapping season identifiers to their metadata, including 'cube_id'.
    output_dir : str, optional
        Directory path to save the history JSON files (default is 'data/processed/history').

    Behavior:
    ---------
    - Creates the output directory if it does not exist.
    - For each season, sends a POST request to fetch the cube's history data.
    - Saves the returned JSON history to a file named '{season}_history.json' in the output directory.
    - Prints success or failure messages for each fetch operation.
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

            print(f"Saved history for {season} to {output_path}")
        else:
            print(f"Failed to fetch history for {season}: {response.status_code}")



def load_combined_history_data(history_dir='data/processed/history'):
    """
    Load and combine 'posts' entries from multiple cube history JSON files in a directory,
    annotating each post with its corresponding season.

    Parameters:
    -----------
    history_dir : str, optional
        Path to the directory containing cube history JSON files (default is 'data/processed/history').

    Returns:
    --------
    list of tuples
        A list of tuples where each tuple contains:
        - season (str): The season identifier extracted from the filename.
        - post (dict): A single post entry from the cube history JSON.

    Raises:
    -------
    ValueError
        If any JSON file does not contain a 'posts' list as expected.

    Behavior:
    ---------
    - Loads all files ending with '_history.json' in sorted order.
    - Extracts and combines 'posts' from each file with the season info.
    - Prints the total number of loaded history entries.
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

    print(f"Loaded {len(all_posts_with_season)} history entries from {history_dir}")
    return all_posts_with_season


def process_history_entries(posts_with_season):
    """
    Process CubeCobra history posts and extract mainboard card changes.

    Parameters:
    -----------
    posts_with_season : list of tuples
        A list of (season, post) tuples, where:
        - season (str): Season identifier.
        - post (dict): A single CubeCobra history post containing changelog info.

    Returns:
    --------
    list of dict
        A list of history records, each representing a card addition or removal with keys:
        - 'season': The season identifier.
        - 'timestamp': The ISO-formatted date string of the change.
        - 'change_type': Either "adds" or "removes".
        - 'scryfallId': The card's unique Scryfall ID.
    
    Notes:
    ------
    - Entries without a timestamp or valid cardID are skipped.
    - Handles both 'adds' and 'removes' changes from the 'mainboard' changelog.
    """
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
    """
    Export processed history change data to a CSV file with incremental change IDs.

    Parameters:
    -----------
    history_data : list of dict
        List of dictionaries representing history change records. Each dict should include:
        - 'season' or 'season_id': Season identifier
        - 'timestamp': ISO date/time string of the change
        - Other relevant fields such as 'change_type' and 'scryfallId'.

    output_csv : str, optional
        File path to save the CSV output (default is 'data/processed/cube_history.csv').

    Behavior:
    ---------
    - Ensures the output directory exists.
    - Converts the input data into a pandas DataFrame.
    - Renames 'season' column to 'season_id' if present.
    - Converts 'timestamp' column to pandas datetime.
    - Sorts the DataFrame by 'timestamp' in ascending order.
    - Adds a new 'change_id' column as an incremental integer starting from 1.
    - Writes the resulting DataFrame to CSV without the index.
    - Prints a confirmation message with the output path.
    """
    import pandas as pd
    import os

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)

    df = pd.DataFrame(history_data)

    if 'season' in df.columns:
        df = df.rename(columns={'season': 'season_id'})

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp').reset_index(drop=True)

    df.insert(0, 'change_id', range(1, len(df) + 1))

    df.to_csv(output_csv, index=False)
    print(f"History changes CSV saved: {output_csv}")

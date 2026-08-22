import requests
import gzip
import os
import pandas as pd
import json

# -----------------------
# Configuration
# -----------------------
SCRYFALL_BULK_URL = "https://api.scryfall.com/bulk-data"
BULK_TYPE = "default_cards"
DRAFTED_DECKS_PATH = "data/processed/drafted_decks.csv"
CUBE_MAINBOARD_PATH = "data/processed/cube_mainboard.csv"
OUTPUT_DIR = "data/cards"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "scryfall_filtered_cards.json.gz")

SCRYFALL_HEADERS = {
    "User-Agent": "ManaCore/1.0",
    "Accept": "application/json;q=0.9,*/*;q=0.8",
}

# -----------------------
# Helper Functions
# -----------------------
def fetch_bulk_file_url(bulk_type: str = BULK_TYPE) -> str:
    """Fetch the download URL for a specific bulk data type."""
    response = requests.get(SCRYFALL_BULK_URL, headers=SCRYFALL_HEADERS)
    response.raise_for_status()
    data = response.json()

    for bulk in data["data"]:
        if bulk["type"] == bulk_type:
            # download_uri (JSON) retired 2026-07-20; jsonl_download_uri is the only one left
            uri = bulk.get("jsonl_download_uri") or bulk.get("download_uri")
            if not uri:
                raise ValueError(f"No download URI in bulk object: {list(bulk)}")
            return uri

    raise ValueError(f"No bulk file found for type '{bulk_type}'")


def download_and_filter_bulk(url: str, drafted_ids: set, save_path: str):
    """Stream the gzipped JSONL bulk file, filter by drafted_ids, save gzip JSON."""
    print("Downloading Scryfall bulk data...")
    filtered_cards = []
    total = 0

    with requests.get(url, headers=SCRYFALL_HEADERS, stream=True) as response:
        response.raise_for_status()
        response.raw.decode_content = True  # handle transport-level Content-Encoding
        with gzip.open(response.raw, "rt", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                total += 1
                card = json.loads(line)
                if card["id"] in drafted_ids:
                    filtered_cards.append(card)

    print(f"Filtered {len(filtered_cards)} cards out of {total} total.")

    with gzip.open(save_path, "wt", encoding="utf-8") as f:
        json.dump(filtered_cards, f)
    print(f"Saved filtered cards to: {save_path}")

# -----------------------
# Main Script
# -----------------------
if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Load drafted deck Scryfall IDs
    print("Loading drafted decks IDs...")
    drafted_decks = pd.read_csv(DRAFTED_DECKS_PATH)
    cube_mainboard = pd.read_csv(CUBE_MAINBOARD_PATH)
    
    # Combine unique IDs from both files
    drafted_ids = set(drafted_decks["scryfallId"].dropna().unique())
    cube_ids = set(cube_mainboard["scryfallId"].dropna().unique())
    drafted_ids = drafted_ids.union(cube_ids)
    
    print(f"Found {len(drafted_ids)} unique drafted card IDs.")

    # Fetch bulk file URL
    print("Fetching Scryfall bulk file URL...")
    bulk_url = fetch_bulk_file_url()

    # Download and filter
    download_and_filter_bulk(bulk_url, drafted_ids, OUTPUT_FILE)

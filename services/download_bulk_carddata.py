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
OUTPUT_DIR = "data/cards"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "scryfall_filtered_cards.json.gz")


# -----------------------
# Helper Functions
# -----------------------
def fetch_bulk_file_url(bulk_type: str = BULK_TYPE) -> str:
    """Fetch the download URL for a specific bulk data type."""
    response = requests.get(SCRYFALL_BULK_URL)
    response.raise_for_status()
    data = response.json()
    
    for bulk in data["data"]:
        if bulk["type"] == bulk_type:
            return bulk["download_uri"]
    
    raise ValueError(f"No bulk file found for type '{bulk_type}'")


def download_and_filter_bulk(url: str, drafted_ids: set, save_path: str):
    """Download the bulk file, filter by drafted_ids, and save gzip JSON."""
    print("Downloading Scryfall bulk data...")
    response = requests.get(url)
    response.raise_for_status()

    # Load full JSON array
    data = response.json()

    # Filter by drafted deck IDs
    filtered_cards = [card for card in data if card["id"] in drafted_ids]
    print(f"Filtered {len(filtered_cards)} cards out of {len(data)} total.")

    # Save filtered cards to gzip JSON
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
    drafted_ids = set(drafted_decks["scryfallId"].dropna().unique())
    print(f"Found {len(drafted_ids)} unique drafted card IDs.")

    # Fetch bulk file URL
    print("Fetching Scryfall bulk file URL...")
    bulk_url = fetch_bulk_file_url()

    # Download and filter
    download_and_filter_bulk(bulk_url, drafted_ids, OUTPUT_FILE)

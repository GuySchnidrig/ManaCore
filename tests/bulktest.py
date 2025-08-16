import gzip
import json


from manacore.deckcolor.set_deckcolor import *
# Path to your filtered cards file
filtered_file = "data/cards/scryfall_filtered_cards.json.gz"

# Open and load the JSON
with gzip.open(filtered_file, "rt", encoding="utf-8") as f:
    cards = json.load(f)

# Basic checks
print(f"Total cards loaded: {len(cards)}")
if len(cards) > 0:
    print("Example card:")
    example = cards[0]
    print(f"Name: {example.get('name')}")
    print(f"Scryfall ID: {example.get('id')}")
    print(f"Colors: {example.get('colors')}")
    
    
    

filtered_path = "data/cards/scryfall_filtered_cards.json.gz"

# Load the filtered card index
card_index = load_scryfall_data(filtered_path)

# Get colors for a specific card
scryfall_id = "1be9d9a4-d7ee-4854-abc2-85cabf993ec9"
colors = get_scryfall_colors_local(scryfall_id, card_index)
print(colors)
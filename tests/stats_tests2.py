import os
import pandas as pd
from manacore.statistics.statistics_calculator import *

# Assuming your code is in a module called 'mana_analysis' or similar:
# from mana_analysis import (
#     load_standings_data, load_drafted_decks, load_cube_history, load_drafts_with_timestamp,
#     calculate_combined_winrates_per_season, build_card_availability_map,
#     calculate_card_mainboard_rate_per_season, calculate_card_match_winrate_per_season,
#     calculate_archetype_match_winrate, calculate_decktype_match_winrate,
#     calculate_most_picked_card_by_player
# )

# For now, I will just assume your functions are in the current namespace

base_path = "data/processed"

# Load data
try:
    standings_df = load_standings_data(base_path)
    decks_df = load_drafted_decks(base_path)
    cube_history_df = load_cube_history(base_path)
    drafts_df = load_drafts_with_timestamp(base_path)
except FileNotFoundError as e:
    print(e)
    exit(1)

# Load match data using your function (assuming it loads all matches as DataFrame)
matches_df = load_match_data(base_path)  # Assuming this exists and works

# Test card availability mapping
card_availability_map = build_card_availability_map(cube_history_df, drafts_df)
print("Card availability map sample:")
for season, drafts in list(card_availability_map.items())[:1]:
    print(f"Season {season}: {list(drafts.items())[:7]}")  # print first 3 drafts and their available cards

# Calculate combined winrates per season
combined_winrates_df = calculate_combined_winrates_per_season(matches_df, decks_df)
print(f"\nCombined Winrates (sample):\n{combined_winrates_df.head()}")

# Calculate card mainboard rates
mainboard_rate_df = calculate_card_mainboard_rate_per_season(decks_df, drafts_df, card_availability_map)
print(f"\nCard Mainboard Rates (sample):\n{mainboard_rate_df.head()}")

# Calculate card match winrate per season
card_match_winrate_df = calculate_card_match_winrate_per_season(matches_df, decks_df, card_availability_map)
print(f"\nCard Match Winrates (sample):\n{card_match_winrate_df.head()}")

# Calculate archetype match winrate
archetype_match_winrate_df = calculate_archetype_match_winrate(matches_df, decks_df)
print(f"\nArchetype Match Winrates (sample):\n{archetype_match_winrate_df.head()}")

# Calculate most picked card by player
most_picked_cards_df = calculate_most_picked_card_by_player(decks_df)
print(f"\nMost Picked Card By Player (sample):\n{most_picked_cards_df.head()}")



print(drafts_df['season_id'].unique())  # Do you see 'Season-1' here?

season_1_drafts = drafts_df[drafts_df['season_id'] == 'Season-4']
print(season_1_drafts)

print(season_1_drafts[['draft_id', 'timestamp']])
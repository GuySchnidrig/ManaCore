import os
import pandas as pd
from datetime import datetime
from collections import defaultdict

from manacore.standings.standings_calculator import load_match_data
from manacore.statistics.statistics_calculator import *

processed_dir = os.path.join("data/processed")
# Loading data
standings_df = load_standings_data(processed_dir)
decks_df = load_drafted_decks(processed_dir)
cube_history_df = load_cube_history(processed_dir)
drafts_df = load_drafts_with_timestamp(processed_dir)
matches_df = load_match_data(processed_dir)
mainboard_df = load_mainboard(processed_dir)

availability_map = build_card_availability_map(drafts_df, mainboard_df, cube_history_df)

print("Number of seasons in availability_map:", len(availability_map))

for season, cards in availability_map.items():
    print(f"Season {season} has {len(cards)} unique cards available")


mainboardrate = calculate_card_mainboard_rate_per_season(decks_df, drafts_df, availability_map)
print("\nMainboard  rate sample:")
print(mainboardrate)

# ---- Calculate match win rate ----
match_winrates = calculate_card_match_winrate_per_season(matches_df, decks_df, availability_map, drafts_df)
print("\nMatch win rate sample:")
print(match_winrates)

# ---- Calculate game win rate ----
game_winrates = calculate_card_game_winrate_per_season(matches_df, decks_df, availability_map, drafts_df)
print("\nGame win rate sample:")
print(game_winrates)


    

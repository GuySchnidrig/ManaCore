import os

from manacore.standings.standings_calculator import load_match_data
from manacore.statistics.statistics_calculator import *

def process_and_save_all_outputs(base_path: str):
    processed_dir = os.path.join(base_path, "data", "processed")
    
    # Loading data
    print("Loading standings data...")
    standings_df = load_standings_data(processed_dir)
    print("Standings data loaded.")
    
    print("Loading drafted decks data...")
    decks_df = load_drafted_decks(processed_dir)
    print("Drafted decks data loaded.")
    
    print("Loading cube history data...")
    cube_history_df = load_cube_history(processed_dir)
    print("Cube history data loaded.")
    
    print("Loading drafts with timestamp...")
    drafts_df = load_drafts_with_timestamp(processed_dir)
    print("Drafts data loaded.")
    
    print("Loading match data...")
    matches_df = load_match_data(processed_dir)
    print("Match data loaded.")
    
    # Processing data
    print("Calculating combined winrates per season...")
    combined_winrates_df = calculate_combined_winrates_per_season(matches_df, decks_df)
    print("Combined winrates calculated.")
    
    print("Building card availability map...")
    mainboard_df = load_mainboard(processed_dir)
    availability_map = build_card_availability_map(drafts_df, mainboard_df, cube_history_df)
    print("Card availability map built.")
    
    print("Calculating card mainboard rate per season...")
    mainboard_rate_df = calculate_card_mainboard_rate_per_season(decks_df, drafts_df, availability_map)
    print("Card mainboard rate calculated.")
    
    print("Calculating card match winrate per season...")
    card_match_winrate_df = calculate_card_match_winrate_per_season(matches_df, decks_df, availability_map, drafts_df)
    print("Card match winrate calculated.")
    
    print("Calculating card game winrate per season...")
    card_game_winrate_df = calculate_card_game_winrate_per_season(matches_df, decks_df, availability_map, drafts_df)
    print("Card game winrate calculated.")
    
    print("Calculating archetype match winrate...")
    archetype_match_winrate_df = calculate_archetype_match_winrate(matches_df, decks_df)
    print("Archetype match winrate calculated.")
    
    print("Calculating archetype game winrate...")
    archetype_game_winrate_df = calculate_archetype_game_winrate(matches_df, decks_df)
    print("Archetype game winrate calculated.")

    print("Calculating per-player archetype winrates...")
    player_archetype_winrates_df = calculate_player_archetype_game_winrate(matches_df, decks_df)
    print("Player-archetype winrates calculated.")
    
    print("Calculating per-player decktype winrates...")
    player_decktype_winrates_df = calculate_player_decktype_game_winrate(matches_df, decks_df)
    print("Player-decktype winrates calculated.")

    print("Calculating most picked card by player...")
    most_picked_card_df = calculate_most_picked_card_by_player(decks_df)
    print("Most picked card calculated.")
    
    print("Calculating decktype match winrate...")
    decktype_match_winrate_df = calculate_decktype_match_winrate(matches_df, decks_df, 'decktype')
    print("Decktype match winrate calculated.")
    
    print("Calculating decktype game winrate...")
    decktype_game_winrate_df = calculate_decktype_game_winrate(matches_df, decks_df, 'decktype')
    print("Decktype game winrate calculated.")
    
    print("Calculating player game and match winrate...")
    player_winrate_stats = calculate_player_game_and_match_stats(matches_df)
    print("Player game and match winrate calculated.")

    print("Calculating player vs player winrate...")
    vs_player_winrate_stats = calculate_vs_player_stats(matches_df)
    print("Player vs player winrate calculated.")
    
    # Saving results
    print("Saving combined winrates per season...")
    combined_winrates_df.to_csv(os.path.join(processed_dir, "combined_winrates_per_season.csv"), index=False)
    print("Saved combined winrates.")
    
    print("Saving card mainboard rate per season...")
    mainboard_rate_df.to_csv(os.path.join(processed_dir, "card_mainboard_rate_per_season.csv"), index=False)
    print("Saved card mainboard rate.")
    
    print("Saving card match winrate per season...")
    card_match_winrate_df.to_csv(os.path.join(processed_dir, "card_match_winrate_per_season.csv"), index=False)
    print("Saved card match winrate.")
    
    print("Saving card game winrate per season...")
    card_game_winrate_df.to_csv(os.path.join(processed_dir, "card_game_winrate_per_season.csv"), index=False)
    print("Saved card game winrate.")
    
    print("Saving archetype match winrate...")
    archetype_match_winrate_df.to_csv(os.path.join(processed_dir, "archetype_match_winrate.csv"), index=False)
    print("Saved archetype match winrate.")
    
    print("Saving archetype game winrate...")
    archetype_game_winrate_df.to_csv(os.path.join(processed_dir, "archetype_game_winrate.csv"), index=False)
    print("Saved archetype game winrate.")

    print("Saving player archetype winrates...")
    player_archetype_winrates_df.to_csv(os.path.join(processed_dir, "player_archetype_winrates.csv"), index=False)
    print("Saved player archetype winrates.")

    print("Saving player decktype winrates...")
    player_decktype_winrates_df.to_csv(os.path.join(processed_dir, "player_decktype_winrates.csv"), index=False)
    print("Saved player decktype winrates.")
    
    print("Saving most picked card by player...")
    most_picked_card_df.to_csv(os.path.join(processed_dir, "most_picked_card_by_player.csv"), index=False)
    print("Saved most picked card.")
    
    print("Saving decktype match winrate...")
    decktype_match_winrate_df.to_csv(os.path.join(processed_dir, "decktype_match_winrate.csv"), index=False)
    print("Saved decktype match winrate.")
    
    print("Saving decktype game winrate...")
    decktype_game_winrate_df.to_csv(os.path.join(processed_dir, "decktype_game_winrate.csv"), index=False)
    print("Saved decktype game winrate.")

    print("Saving player game and match winrate...")
    player_winrate_stats.to_csv(os.path.join(processed_dir, "player_game_and_match_winrate.csv"), index=False)
    print("Saved player game and match winrate.")

    print("Saving player vs player winrate...")
    vs_player_winrate_stats.to_csv(os.path.join(processed_dir, "vs_player_game_and_match_winrate.csv"), index=False)
    print("Saved player vs player winrate.")
    
    print(f"All outputs saved to {processed_dir}")

if __name__ == "__main__":
    process_and_save_all_outputs(".")

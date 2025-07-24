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

def calculate_card_match_winrate_per_season(matches_df, decks_df, card_availability_map, drafts_df):
    # Deduplicate decks
    card_drafts = decks_df[['season_id', 'draft_id', 'player', 'scryfallId']].drop_duplicates(
        subset=['season_id', 'draft_id', 'player', 'scryfallId']
    )

    valid_seasons = set(drafts_df['season_id'].unique())

    availability_rows = []
    for season, cards in card_availability_map.items():
        if season in valid_seasons:
            for card in cards:
                availability_rows.append({'season_id': season, 'scryfallId': card})
    availability_df = pd.DataFrame(availability_rows)

    valid_card_drafts = card_drafts.merge(
        availability_df,
        on=['season_id', 'scryfallId'],
        how='inner'
    )

    merged_p1 = matches_df.merge(
        valid_card_drafts.rename(columns={'player': 'player1'}),
        on=['season_id', 'draft_id', 'player1'],
        how='left'
    ).rename(columns={'scryfallId': 'card_p1'})

    merged_p2 = matches_df.merge(
        valid_card_drafts.rename(columns={'player': 'player2'}),
        on=['season_id', 'draft_id', 'player2'],
        how='left'
    ).rename(columns={'scryfallId': 'card_p2'})

    # Include match_id for uniqueness
    p1_long = merged_p1[['season_id', 'match_id', 'card_p1', 'player1Wins', 'player2Wins']].rename(columns={'card_p1': 'scryfallId'})
    p2_long = merged_p2[['season_id', 'match_id', 'card_p2', 'player2Wins', 'player1Wins']].rename(columns={'card_p2': 'scryfallId'})

    p1_long = p1_long.dropna(subset=['scryfallId'])
    p2_long = p2_long.dropna(subset=['scryfallId'])

    p1_long['matches_played'] = 1
    p1_long['matches_won'] = (p1_long['player1Wins'] > p1_long['player2Wins']).astype(int)

    p2_long['matches_played'] = 1
    p2_long['matches_won'] = (p2_long['player2Wins'] > p2_long['player1Wins']).astype(int)

    all_players = pd.concat([p1_long, p2_long], ignore_index=True)

    # Drop duplicates on season_id, match_id, card to avoid double counting
    all_players = all_players.drop_duplicates(subset=['season_id', 'match_id', 'scryfallId'])

    aggregated = all_players.groupby(['season_id', 'scryfallId']).agg(
        matches_played=('matches_played', 'sum'),
        matches_won=('matches_won', 'sum')
    ).reset_index()

    full_df = availability_df.merge(
        aggregated,
        on=['season_id', 'scryfallId'],
        how='left'
    ).fillna({'matches_played': 0, 'matches_won': 0})

    full_df['match_win_rate'] = full_df.apply(
        lambda row: row['matches_won'] / row['matches_played'] if row['matches_played'] > 0 else 0,
        axis=1
    )

    return full_df

card_id = 'd2124603-d20e-40eb-97f0-a66323397ac2'

print("Number of deck rows with this card:", decks_df[decks_df['scryfallId'] == card_id].shape[0])
print(decks_df[decks_df['scryfallId'] == card_id].head())

# Get all player/draft/season combos with the card
players_with_card = decks_df[decks_df['scryfallId'] == card_id][['season_id', 'draft_id', 'player']].drop_duplicates()

# Check matches where player1 has the card
matches_with_p1_card = matches_df.merge(
    players_with_card.rename(columns={'player': 'player1'}),
    on=['season_id', 'draft_id', 'player1'],
    how='inner'
)

# Check matches where player2 has the card
matches_with_p2_card = matches_df.merge(
    players_with_card.rename(columns={'player': 'player2'}),
    on=['season_id', 'draft_id', 'player2'],
    how='inner'
)

print("Matches where player1 has the card:", matches_with_p1_card.shape[0])
print("Matches where player2 has the card:", matches_with_p2_card.shape[0])
print("Total matches with card (union):", pd.concat([matches_with_p1_card, matches_with_p2_card]).drop_duplicates().shape[0])

# Player1 wins with the card
p1_wins = (matches_with_p1_card['player1Wins'] > matches_with_p1_card['player2Wins']).sum()

# Player2 wins with the card
p2_wins = (matches_with_p2_card['player2Wins'] > matches_with_p2_card['player1Wins']).sum()

print(f"Player1 wins with card: {p1_wins}")
print(f"Player2 wins with card: {p2_wins}")
print(f"Total wins counted for card: {p1_wins + p2_wins}")


# Run the fixed function
final_df = calculate_card_match_winrate_per_season(matches_df, decks_df, availability_map, drafts_df)

# Filter for your specific card
card_stats = final_df[final_df['scryfallId'] == card_id]

print(card_stats[['season_id', 'scryfallId', 'matches_played', 'matches_won', 'match_win_rate']])




card_id = 'd2124603-d20e-40eb-97f0-a66323397ac2'

players_with_card = decks_df[decks_df['scryfallId'] == card_id][['season_id', 'draft_id', 'player']].drop_duplicates()

matches_with_p1_card = matches_df.merge(
    players_with_card.rename(columns={'player': 'player1'}),
    on=['season_id', 'draft_id', 'player1'],
    how='inner'
)

matches_with_p2_card = matches_df.merge(
    players_with_card.rename(columns={'player': 'player2'}),
    on=['season_id', 'draft_id', 'player2'],
    how='inner'
)

# Find matches where both players have the card
both_have_card = pd.merge(
    matches_with_p1_card[['match_id']],
    matches_with_p2_card[['match_id']],
    on='match_id'
)['match_id']

# Filter out these matches from player2 to avoid double counting
matches_with_p2_card_filtered = matches_with_p2_card[~matches_with_p2_card['match_id'].isin(both_have_card)]

p1_games_played = matches_with_p1_card[['player1Wins', 'player2Wins', 'draws']].sum(axis=1).sum()
p1_games_won = matches_with_p1_card['player1Wins'].sum()

p2_games_played_filtered = matches_with_p2_card_filtered[['player1Wins', 'player2Wins', 'draws']].sum(axis=1).sum()
p2_games_won_filtered = matches_with_p2_card_filtered['player2Wins'].sum()

print(f"Player1 games won with card: {p1_games_won}")
print(f"Player2 games played with card (filtered): {p2_games_played_filtered}")
print(f"Player2 games won with card (filtered): {p2_games_won_filtered}")
print(f"Total games played with card (no double count): {p1_games_played + p2_games_played_filtered}")
print(f"Total games won with card (no double count): {p1_games_won + p2_games_won_filtered}")

card_game_winrate_df = calculate_card_game_winrate_per_season(matches_df, decks_df, availability_map, drafts_df)

print(card_game_winrate_df[card_game_winrate_df['scryfallId'] == card_id])
import os
import pandas as pd
from datetime import datetime
from collections import defaultdict

from manacore.standings.standings_calculator import load_match_data


def load_standings_data(base_path: str) -> pd.DataFrame:
    standings_file = os.path.join(base_path, "standings.csv")
    if not os.path.exists(standings_file):
        raise FileNotFoundError(f"'standings.csv' not found in {base_path}")
    return pd.read_csv(standings_file)


def load_drafted_decks(base_path: str) -> pd.DataFrame:
    decks_file = os.path.join(base_path, "drafted_decks.csv")
    if not os.path.exists(decks_file):
        raise FileNotFoundError(f"'drafted_decks.csv' not found in {base_path}")
    df = pd.read_csv(decks_file)
    return df


def load_cube_history(base_path: str) -> pd.DataFrame:
    cube_history_file = os.path.join(base_path, "cube_history.csv")
    if not os.path.exists(cube_history_file):
        raise FileNotFoundError(f"'cube_history.csv' not found in {base_path}")
    df = pd.read_csv(cube_history_file)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

def load_mainboard(base_path: str) -> pd.DataFrame:
    load_mainboard_file = os.path.join(base_path, "cube_mainboard.csv")
    if not os.path.exists(load_mainboard_file):
        raise FileNotFoundError(f"'cube_history.csv' not found in {base_path}")
    df = pd.read_csv(load_mainboard_file)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df


def load_drafts_with_timestamp(base_path: str) -> pd.DataFrame:
    drafts_file = os.path.join(base_path, "drafts.csv")
    if not os.path.exists(drafts_file):
        raise FileNotFoundError(f"'drafts.csv' not found in {base_path}")
    df = pd.read_csv(drafts_file)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df


def calculate_combined_winrates_per_season(matches_df: pd.DataFrame, decks_df: pd.DataFrame) -> pd.DataFrame:
    results = []

    # All (player, draft_id, scryfallId) rows
    grouped_decks = decks_df[['player', 'draft_id', 'scryfallId']].drop_duplicates()
    player_card_combos = grouped_decks[['player', 'scryfallId']].drop_duplicates()

    for _, row in player_card_combos.iterrows():
        player = row['player']
        card = row['scryfallId']

        # Drafts where this player drafted this card
        player_drafts_with_card = grouped_decks[
            (grouped_decks['player'] == player) & 
            (grouped_decks['scryfallId'] == card)
        ]['draft_id'].unique()

        # Count how many drafts this card was used in by the player
        num_drafts_with_card = len(player_drafts_with_card)

        # Filter matches only from those drafts AND where player was involved
        matches_with_card = matches_df[
            (matches_df['draft_id'].isin(player_drafts_with_card)) &
            ((matches_df['player1'] == player) | (matches_df['player2'] == player))
        ]

        if matches_with_card.empty:
            continue

        # Group matches by season
        for season_id, season_matches in matches_with_card.groupby('season_id'):
            player_matches = season_matches[
                (season_matches['player1'] == player) | (season_matches['player2'] == player)
            ]
            matches_played = len(player_matches)

            # Match wins
            player1_wins = (player_matches['player1'] == player) & (player_matches['player1Wins'] > player_matches['player2Wins'])
            player2_wins = (player_matches['player2'] == player) & (player_matches['player2Wins'] > player_matches['player1Wins'])
            matches_won = (player1_wins | player2_wins).sum()
            match_win_rate = matches_won / matches_played if matches_played > 0 else 0.0

            # Game stats
            games_played = (
                player_matches['player1Wins'] +
                player_matches['player2Wins'] +
                player_matches['draws']
            ).sum()

            games_won = (
                player_matches.loc[player_matches['player1'] == player, 'player1Wins'].sum() +
                player_matches.loc[player_matches['player2'] == player, 'player2Wins'].sum()
            )

            game_win_rate = games_won / games_played if games_played > 0 else 0.0

            results.append({
                'player': player,
                'scryfallId': card,
                'season_id': season_id,
                'num_drafts_with_card': num_drafts_with_card,
                'matches_played': matches_played,
                'matches_won': matches_won,
                'match_win_rate': match_win_rate,
                'games_played': games_played,
                'games_won': games_won,
                'game_win_rate': game_win_rate
            })

    return pd.DataFrame(results)


def build_card_availability_map(cube_history_df, drafts_df, mainboard_df):
    # Convert timestamps to datetime if needed
    
    cube_history_df = cube_history_df.sort_values('timestamp', ascending=False).reset_index(drop=True)
    drafts_df = drafts_df.sort_values('timestamp', ascending=False).reset_index(drop=True)
    mainboard_df = mainboard_df.sort_values('timestamp').reset_index(drop=True)

    availability_map = {}

    for season, mainboard_cards in mainboard_df.groupby('season_id'):
        # Initialize available cards from mainboard snapshot at mainboard timestamp
        mainboard_timestamp = mainboard_cards['timestamp'].iloc[0]
        available_cards = set(mainboard_cards['scryfallId'])

        # Filter changes and drafts for this season
        season_changes = cube_history_df[cube_history_df['season_id'] == season]
        season_drafts = drafts_df[drafts_df['season_id'] == season]

        idx_change = 0
        availability_map[season] = {}

        # Sort drafts descending (latest draft first)
        season_drafts = season_drafts.sort_values('timestamp', ascending=False).reset_index(drop=True)

        for _, draft in season_drafts.iterrows():
            draft_time = draft['timestamp']
            draft_id = draft['draft_id']

            # Move idx_change through changes happening after this draft but before mainboard snapshot
            while idx_change < len(season_changes) and season_changes.iloc[idx_change]['timestamp'] > draft_time:
                change = season_changes.iloc[idx_change]
                card_id = change['scryfallId']
                if change['change_type'] == 'adds':
                    # Card was added later, so before that it was NOT available — remove it now going backward
                    available_cards.discard(card_id)
                elif change['change_type'] == 'removes':
                    # Card was removed later, so before that it WAS available — add it back going backward
                    available_cards.add(card_id)
                idx_change += 1

            availability_map[season][draft_id] = available_cards.copy()

    return availability_map

def calculate_card_mainboard_rate_per_season(decks_df: pd.DataFrame, drafts_df: pd.DataFrame, card_availability_map: dict) -> pd.DataFrame:
    card_drafts = decks_df.groupby(['season_id', 'draft_id', 'scryfallId']).size().reset_index().drop(0, axis=1)
    drafts_per_season = drafts_df.groupby('season_id')['draft_id'].nunique().reset_index()
    drafts_per_season.rename(columns={'draft_id': 'total_drafts_in_season'}, inplace=True)

    results = defaultdict(lambda: {'drafts_with_card': 0, 'total_drafts_in_season': 0})

    for _, row in card_drafts.iterrows():
        season = row['season_id']
        draft_id = row['draft_id']
        card = row['scryfallId']

        if season not in card_availability_map or draft_id not in card_availability_map[season]:
            continue
        if card not in card_availability_map[season][draft_id]:
            continue

        key = (season, card)
        results[key]['drafts_with_card'] += 1

    for (season, card), data in results.items():
        total = drafts_per_season.loc[drafts_per_season['season_id'] == season, 'total_drafts_in_season']
        data['total_drafts_in_season'] = int(total.values[0]) if not total.empty else 0
        data['season_id'] = season
        data['scryfallId'] = card
        data['mainboard_rate'] = data['drafts_with_card'] / data['total_drafts_in_season'] if data['total_drafts_in_season'] > 0 else 0

    df = pd.DataFrame(results.values())
    return df[['season_id', 'scryfallId', 'drafts_with_card', 'total_drafts_in_season', 'mainboard_rate']]


def calculate_card_match_winrate_per_season(matches_df, decks_df, card_availability_map):
    # Deduplicate decks to unique cards per player per draft per season
    card_drafts = decks_df[['season_id', 'draft_id', 'player', 'scryfallId']].drop_duplicates()

    # Create DataFrame from card_availability_map for fast lookup
    # Assuming card_availability_map structure: {season_id: {draft_id: set(cards)}}
    availability_rows = []
    for season, drafts in card_availability_map.items():
        for draft_id, cards in drafts.items():
            for card in cards:
                availability_rows.append({'season_id': season, 'draft_id': draft_id, 'scryfallId': card})
    availability_df = pd.DataFrame(availability_rows)

    # Join card_drafts with availability to keep only cards available in that draft & season
    valid_card_drafts = card_drafts.merge(
        availability_df,
        on=['season_id', 'draft_id', 'scryfallId'],
        how='inner'
    )

    # Merge matches with valid_card_drafts twice: once for player1, once for player2, to get card presence per player
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

    # Combine presence info
    combined = merged_p1[['season_id', 'draft_id', 'player1', 'player2', 'player1Wins', 'player2Wins', 'card_p1']].copy()
    combined['card_p2'] = merged_p2['card_p2']

    # Melt so each row is a player-card match
    p1_long = combined[['season_id', 'card_p1', 'player1Wins', 'player2Wins']].rename(columns={'card_p1': 'scryfallId'})
    p2_long = combined[['season_id', 'card_p2', 'player2Wins', 'player1Wins']].rename(columns={'card_p2': 'scryfallId'})

    p1_long = p1_long.dropna(subset=['scryfallId'])
    p2_long = p2_long.dropna(subset=['scryfallId'])

    p1_long['matches_played'] = 1
    p1_long['matches_won'] = (p1_long['player1Wins'] > p1_long['player2Wins']).astype(int)

    p2_long['matches_played'] = 1
    p2_long['matches_won'] = (p2_long['player2Wins'] > p2_long['player1Wins']).astype(int)

    all_players = pd.concat([p1_long, p2_long], ignore_index=True)

    # Aggregate by season and card
    result = all_players.groupby(['season_id', 'scryfallId']).agg(
        matches_played=('matches_played', 'sum'),
        matches_won=('matches_won', 'sum')
    ).reset_index()

    result['match_win_rate'] = result['matches_won'] / result['matches_played']

    return result




def calculate_card_game_winrate_per_season(matches_df: pd.DataFrame, decks_df: pd.DataFrame, card_availability_map: dict) -> pd.DataFrame:
    # Deduplicate decks to unique cards per player per draft per season
    card_drafts = decks_df[['season_id', 'draft_id', 'player', 'scryfallId']].drop_duplicates()

    # Create availability DataFrame from card_availability_map
    availability_rows = []
    for season, drafts in card_availability_map.items():
        for draft_id, cards in drafts.items():
            for card in cards:
                availability_rows.append({'season_id': season, 'draft_id': draft_id, 'scryfallId': card})
    availability_df = pd.DataFrame(availability_rows)

    # Filter card drafts by availability
    valid_card_drafts = card_drafts.merge(
        availability_df,
        on=['season_id', 'draft_id', 'scryfallId'],
        how='inner'
    )

    # Merge matches with valid_card_drafts twice to get cards per player per match
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

    # Combine card presence info
    combined = merged_p1[['season_id', 'draft_id', 'player1', 'player2', 'player1Wins', 'player2Wins', 'draws', 'card_p1']].copy()
    combined['card_p2'] = merged_p2['card_p2']

    # Prepare long format for each player-card pair with game results
    p1_long = combined[['season_id', 'card_p1', 'player1Wins', 'player2Wins', 'draws']].rename(columns={'card_p1': 'scryfallId'})
    p2_long = combined[['season_id', 'card_p2', 'player2Wins', 'player1Wins', 'draws']].rename(columns={'card_p2': 'scryfallId'})

    p1_long = p1_long.dropna(subset=['scryfallId'])
    p2_long = p2_long.dropna(subset=['scryfallId'])

    # Calculate games played and games won for player1 perspective
    p1_long['games_played'] = p1_long['player1Wins'] + p1_long['player2Wins'] + p1_long['draws']
    p1_long['games_won'] = p1_long['player1Wins']

    # Calculate games played and games won for player2 perspective
    p2_long['games_played'] = p2_long['player1Wins'] + p2_long['player2Wins'] + p2_long['draws']
    p2_long['games_won'] = p2_long['player2Wins']

    # Combine both players' data
    all_players = pd.concat([p1_long, p2_long], ignore_index=True)

    # Aggregate by season and card to calculate total games played and games won
    result = all_players.groupby(['season_id', 'scryfallId']).agg(
        games_played=('games_played', 'sum'),
        games_won=('games_won', 'sum')
    ).reset_index()

    result['game_win_rate'] = result['games_won'] / result['games_played']

    return result




def calculate_archetype_match_winrate(matches_df: pd.DataFrame, decks_df: pd.DataFrame) -> pd.DataFrame:
    # First get unique archetypes per player per draft + season to avoid duplicates
    player_archetypes = decks_df[['season_id', 'draft_id', 'player', 'archetype']].drop_duplicates()

    # Merge archetype info for player1 and player2 in matches
    merged = matches_df.merge(
        player_archetypes.rename(columns={'player': 'player1', 'archetype': 'archetype_p1'}),
        on=['season_id', 'draft_id', 'player1'], how='left'
    ).merge(
        player_archetypes.rename(columns={'player': 'player2', 'archetype': 'archetype_p2'}),
        on=['season_id', 'draft_id', 'player2'], how='left'
    )

    # Create long format: one row per player per match with their archetype, wins, and whether they won
    p1 = merged[['season_id', 'archetype_p1', 'player1Wins', 'player2Wins']].rename(
        columns={'archetype_p1': 'archetype', 'player1Wins': 'wins', 'player2Wins': 'losses'})
    p2 = merged[['season_id', 'archetype_p2', 'player2Wins', 'player1Wins']].rename(
        columns={'archetype_p2': 'archetype', 'player2Wins': 'wins', 'player1Wins': 'losses'})

    combined = pd.concat([p1, p2], ignore_index=True).dropna(subset=['archetype'])

    combined['matches_played'] = 1
    combined['matches_won'] = (combined['wins'] > combined['losses']).astype(int)

    # Aggregate by season and archetype
    result = combined.groupby(['season_id', 'archetype']).agg(
        matches_played=('matches_played', 'sum'),
        matches_won=('matches_won', 'sum')
    ).reset_index()

    result['match_win_rate'] = result['matches_won'] / result['matches_played']

    return result

def calculate_archetype_game_winrate(matches_df: pd.DataFrame, decks_df: pd.DataFrame) -> pd.DataFrame:
    # Deduplicate to unique player archetypes per draft + season
    player_archetypes = decks_df[['season_id', 'draft_id', 'player', 'archetype']].drop_duplicates()

    # Merge archetype info for player1 and player2 in matches
    merged = matches_df.merge(
        player_archetypes.rename(columns={'player': 'player1', 'archetype': 'archetype_p1'}),
        on=['season_id', 'draft_id', 'player1'], how='left'
    ).merge(
        player_archetypes.rename(columns={'player': 'player2', 'archetype': 'archetype_p2'}),
        on=['season_id', 'draft_id', 'player2'], how='left'
    )

    # Prepare data for player1 and player2 in long format
    p1 = merged[['season_id', 'archetype_p1', 'player1Wins', 'player2Wins', 'draws']].rename(
        columns={'archetype_p1': 'archetype', 'player1Wins': 'wins', 'player2Wins': 'losses'})
    p2 = merged[['season_id', 'archetype_p2', 'player2Wins', 'player1Wins', 'draws']].rename(
        columns={'archetype_p2': 'archetype', 'player2Wins': 'wins', 'player1Wins': 'losses'})

    combined = pd.concat([p1, p2], ignore_index=True).dropna(subset=['archetype'])

    # Calculate total games played per row (wins + losses + draws)
    combined['games_played'] = combined['wins'] + combined['losses'] + combined.get('draws', 0)

    combined['games_won'] = combined['wins']

    # Aggregate by season and archetype
    result = combined.groupby(['season_id', 'archetype']).agg(
        games_played=('games_played', 'sum'),
        games_won=('games_won', 'sum')
    ).reset_index()

    result['game_win_rate'] = result['games_won'] / result['games_played']

    return result

def calculate_most_picked_card_by_player(decks_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the most picked card by each player.

    Args:
        decks_df: DataFrame with drafted decks data, must contain columns ['player', 'scryfallId']

    Returns:
        DataFrame with columns ['player', 'scryfallId', 'pick_count'] containing each player's most picked card and how many times they picked it.
    """
    # Count number of times each player picked each card
    picks = decks_df.groupby(['player', 'scryfallId']).size().reset_index(name='pick_count')

    # For each player, find the card with the max pick_count
    idx = picks.groupby('player')['pick_count'].idxmax()

    most_picked = picks.loc[idx].reset_index(drop=True)

    return most_picked


def calculate_decktype_match_winrate(matches_df, draftedecks_df, decktype_level='decktype'):
    # Reduce to unique player decktypes per draft
    player_decktypes = draftedecks_df[['season_id', 'draft_id', 'player', decktype_level]].drop_duplicates()

    # Merge decktype info for both players into matches
    merged = matches_df.merge(
        player_decktypes.rename(columns={'player': 'player1', decktype_level: 'decktype_p1'}),
        on=['season_id', 'draft_id', 'player1'], how='left'
    ).merge(
        player_decktypes.rename(columns={'player': 'player2', decktype_level: 'decktype_p2'}),
        on=['season_id', 'draft_id', 'player2'], how='left'
    )

    # Determine winner decktype
    def get_winner_decktype(row):
        if row['player1Wins'] > row['player2Wins']:
            return row['decktype_p1']
        elif row['player2Wins'] > row['player1Wins']:
            return row['decktype_p2']
        else:
            return None

    merged['winner_decktype'] = merged.apply(get_winner_decktype, axis=1)

    # Prepare player-centric data
    p1 = merged[['season_id', 'draft_id', 'decktype_p1', 'player1Wins', 'player2Wins']].rename(columns={'decktype_p1': decktype_level})
    p2 = merged[['season_id', 'draft_id', 'decktype_p2', 'player2Wins', 'player1Wins']].rename(
        columns={'decktype_p2': decktype_level, 'player2Wins': 'player1Wins', 'player1Wins': 'player2Wins'}
    )

    all_players = pd.concat([p1, p2], ignore_index=True).dropna(subset=[decktype_level])

    all_players['won'] = all_players['player1Wins'] > all_players['player2Wins']

    # Group and calculate
    result = all_players.groupby(['season_id', decktype_level]).agg(
        matches_played=('draft_id', 'count'),
        matches_won=('won', 'sum')
    ).reset_index()

    result['match_win_rate'] = result['matches_won'] / result['matches_played']

    return result


def calculate_decktype_game_winrate(matches_df, decks_df, decktype_column):
    """
    Calculate decktype game winrate as games won / games played.

    Parameters:
    - matches_df: DataFrame with ['player1', 'player2', 'player1Wins', 'player2Wins', 'draft_id']
    - decks_df: DataFrame with ['draft_id', 'player', decktype_column]
    - decktype_column: str, column name for decktype level (e.g., 'decktype')

    Returns:
    - DataFrame with decktype and their game winrate.
    """

    # First reduce decks_df to unique player-draft decktypes to avoid duplication
    unique_decks = decks_df[['draft_id', 'player', decktype_column]].drop_duplicates()

    # Merge decktype info onto player1 and player2 in matches
    merged_p1 = matches_df.merge(
        unique_decks.rename(columns={'player': 'player1', decktype_column: f'{decktype_column}_p1'}),
        on=['draft_id', 'player1'],
        how='left'
    )
    merged_p2 = matches_df.merge(
        unique_decks.rename(columns={'player': 'player2', decktype_column: f'{decktype_column}_p2'}),
        on=['draft_id', 'player2'],
        how='left'
    )

    # Calculate total games played per row (match) = sum of wins by both players
    merged_p1['total_games'] = merged_p1['player1Wins'] + merged_p1['player2Wins']
    merged_p2['total_games'] = merged_p2['player1Wins'] + merged_p2['player2Wins']

    # Aggregate games won and games played by decktype for player1 decks
    p1_stats = merged_p1.groupby(f'{decktype_column}_p1').agg(
        games_won=('player1Wins', 'sum'),
        games_played=('total_games', 'sum')
    ).reset_index().rename(columns={f'{decktype_column}_p1': decktype_column})

    # Aggregate games won and games played by decktype for player2 decks
    p2_stats = merged_p2.groupby(f'{decktype_column}_p2').agg(
        games_won=('player2Wins', 'sum'),
        games_played=('total_games', 'sum')
    ).reset_index().rename(columns={f'{decktype_column}_p2': decktype_column})

    # Combine stats from both players
    combined = pd.merge(p1_stats, p2_stats, on=decktype_column, how='outer', suffixes=('_p1', '_p2')).fillna(0)

    # Sum wins and games played across both player1 and player2 decks
    combined['games_won_total'] = combined['games_won_p1'] + combined['games_won_p2']
    combined['games_played_total'] = combined['games_played_p1'] + combined['games_played_p2']

    # Calculate winrate
    combined['game_winrate'] = combined['games_won_total'] / combined['games_played_total']

    return combined[[decktype_column, 'games_won_total', 'games_played_total', 'game_winrate']]

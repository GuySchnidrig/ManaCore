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


def build_card_availability_map(drafts_df,mainboard_df, cube_history_df):
    availability_map = {}

    # Group mainboard and history by season
    for season in mainboard_df['season_id'].unique():
        # Start with cards from mainboard snapshot
        mainboard_cards = mainboard_df[mainboard_df['season_id'] == season]
        cards_from_mainboard = set(mainboard_cards['scryfallId'])

        # Add any card that appeared in cube history for this season
        season_changes = cube_history_df[cube_history_df['season_id'] == season]
        cards_from_changes = set(season_changes['scryfallId'])

        # Union of both sources
        all_cards = cards_from_mainboard.union(cards_from_changes)

        availability_map[season] = all_cards

    return availability_map

def calculate_card_mainboard_rate_per_season(decks_df: pd.DataFrame, drafts_df: pd.DataFrame, card_availability_map: dict) -> pd.DataFrame:
    # Count how many drafts each card appeared in
    card_drafts = decks_df.groupby(['season_id', 'draft_id', 'scryfallId']).size().reset_index().drop(0, axis=1)
    drafts_per_season = drafts_df.groupby('season_id')['draft_id'].nunique().reset_index()
    drafts_per_season.rename(columns={'draft_id': 'total_drafts_in_season'}, inplace=True)

    # Count drafts with card
    drafts_with_card = card_drafts.groupby(['season_id', 'scryfallId'])['draft_id'].nunique().reset_index()
    drafts_with_card.rename(columns={'draft_id': 'drafts_with_card'}, inplace=True)

    # Prepare result rows
    results = []

    for season, total_row in drafts_per_season.iterrows():
        season_id = total_row['season_id']
        total_drafts = total_row['total_drafts_in_season']

        # All cards available in this season
        all_cards = card_availability_map.get(season_id, set())

        # Filter drafts_with_card for this season
        season_cards = drafts_with_card[drafts_with_card['season_id'] == season_id].set_index('scryfallId')

        for card in all_cards:
            drafts_count = 0
            if card in season_cards.index:
                drafts_count = season_cards.loc[card, 'drafts_with_card']
            mainboard_rate = drafts_count / total_drafts if total_drafts > 0 else 0

            results.append({
                'season_id': season_id,
                'scryfallId': card,
                'drafts_with_card': drafts_count,
                'total_drafts_in_season': total_drafts,
                'mainboard_rate': mainboard_rate
            })

    df = pd.DataFrame(results)
    return df[['season_id', 'scryfallId', 'drafts_with_card', 'total_drafts_in_season', 'mainboard_rate']]


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


def calculate_card_game_winrate_per_season(matches_df, decks_df, card_availability_map, drafts_df):
    # Deduplicate decks to unique cards per player per draft per season
    card_drafts = decks_df[['season_id', 'draft_id', 'player', 'scryfallId']].drop_duplicates()

    # Get only seasons present in drafts_df
    valid_seasons = set(drafts_df['season_id'].unique())

    # Create availability DataFrame from card_availability_map but only for valid seasons
    availability_rows = []
    for season, cards in card_availability_map.items():
        if season in valid_seasons:
            for card in cards:
                availability_rows.append({'season_id': season, 'scryfallId': card})
    availability_df = pd.DataFrame(availability_rows)

    # Filter card drafts by availability and deduplicate again
    valid_card_drafts = card_drafts.merge(
        availability_df,
        on=['season_id', 'scryfallId'],
        how='inner'
    ).drop_duplicates(subset=['season_id', 'draft_id', 'player', 'scryfallId'])

    # Melt matches_df to get player rows: each row corresponds to a player in a match
    p1 = matches_df[['season_id', 'match_id', 'draft_id', 'player1', 'player2', 'player1Wins', 'player2Wins', 'draws']].copy()
    p1 = p1.rename(columns={
        'player1': 'player',
        'player2': 'opponent',
        'player1Wins': 'playerWins',
        'player2Wins': 'opponentWins'
    })
    p1['player_position'] = 'player1'

    p2 = matches_df[['season_id', 'match_id', 'draft_id', 'player1', 'player2', 'player1Wins', 'player2Wins', 'draws']].copy()
    p2 = p2.rename(columns={
        'player2': 'player',
        'player1': 'opponent',
        'player2Wins': 'playerWins',
        'player1Wins': 'opponentWins'
    })
    p2['player_position'] = 'player2'

    matches_long = pd.concat([p1, p2], ignore_index=True)

    # Merge with valid_card_drafts to get cards per player in each match
    merged = matches_long.merge(
        valid_card_drafts,
        on=['season_id', 'draft_id', 'player'],
        how='inner'
    )

    # Calculate games played and games won from each player's perspective
    merged['games_played'] = merged['playerWins'] + merged['opponentWins'] + merged['draws']
    merged['games_won'] = merged['playerWins']

    # Aggregate per season, card to get total games played and won
    aggregated = merged.groupby(['season_id', 'scryfallId']).agg(
        games_played=('games_played', 'sum'),
        games_won=('games_won', 'sum')
    ).reset_index()

    # Merge with availability to include cards with zero games played
    full_df = availability_df.merge(
        aggregated,
        on=['season_id', 'scryfallId'],
        how='left'
    ).fillna({'games_played': 0, 'games_won': 0})

    full_df['game_win_rate'] = full_df.apply(
        lambda row: row['games_won'] / row['games_played'] if row['games_played'] > 0 else 0,
        axis=1
    )

    return full_df






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
    result = result[['season_id', 'archetype', 'matches_won', 'matches_played', 'match_win_rate']]
    return result

def calculate_archetype_game_winrate(matches_df: pd.DataFrame, decks_df: pd.DataFrame) -> pd.DataFrame:
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
    
    # FIXED: Include draws in total games calculation
    merged['total_games_per_match'] = merged['player1Wins'] + merged['player2Wins'] + merged['draws'].fillna(0)
    
    # Create long format: one row per player per match with their archetype and game counts
    p1 = merged[['season_id', 'archetype_p1', 'player1Wins', 'total_games_per_match']].rename(
        columns={'archetype_p1': 'archetype', 'player1Wins': 'games_won'})
    p2 = merged[['season_id', 'archetype_p2', 'player2Wins', 'total_games_per_match']].rename(
        columns={'archetype_p2': 'archetype', 'player2Wins': 'games_won'})
    
    combined = pd.concat([p1, p2], ignore_index=True).dropna(subset=['archetype'])
    
    # FIXED: Use total_games_per_match instead of calculating games_won + games_lost
    # This ensures draws are included in games_played
    
    # Aggregate by season and archetype
    result = combined.groupby(['season_id', 'archetype']).agg(
        games_played=('total_games_per_match', 'sum'),  # Now includes draws
        games_won=('games_won', 'sum')                  # Draws are not wins
    ).reset_index()

    result['game_win_rate'] = result['games_won'] / result['games_played']
    result = result[['season_id', 'archetype', 'games_won', 'games_played', 'game_win_rate']]
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
    result = result[['season_id', decktype_level, 'matches_won', 'matches_played', 'match_win_rate']]
    return result


def calculate_decktype_game_winrate(matches_df, decks_df, decktype_column):
    """
    Calculate decktype game winrate as games won / games played.
    FIXED: Now properly includes draws in total games played and handles season_id.
    
    Parameters:
    - matches_df: DataFrame with ['season_id', 'draft_id', 'player1', 'player2', 'player1Wins', 'player2Wins', 'draws']
    - decks_df: DataFrame with ['season_id', 'draft_id', 'player', decktype_column]
    - decktype_column: str, column name for decktype level (e.g., 'decktype')
    
    Returns:
    - DataFrame with season_id, decktype and their game winrate.
    """
    # First reduce decks_df to unique player-draft decktypes to avoid duplication
    unique_decks = decks_df[['season_id', 'draft_id', 'player', decktype_column]].drop_duplicates()
    
    # Merge decktype info onto player1 and player2 in matches
    merged_p1 = matches_df.merge(
        unique_decks.rename(columns={'player': 'player1', decktype_column: f'{decktype_column}_p1'}),
        on=['season_id', 'draft_id', 'player1'],  # Include season_id in merge
        how='left'
    )
    merged_p2 = matches_df.merge(
        unique_decks.rename(columns={'player': 'player2', decktype_column: f'{decktype_column}_p2'}),
        on=['season_id', 'draft_id', 'player2'],  # Include season_id in merge
        how='left'
    )
    
    # FIXED: Calculate total games played per row (match) INCLUDING DRAWS
    # Handle case where 'draws' column might not exist or have NaN values
    if 'draws' not in matches_df.columns:
        merged_p1['draws'] = 0
        merged_p2['draws'] = 0
    else:
        merged_p1['draws'] = merged_p1['draws'].fillna(0)
        merged_p2['draws'] = merged_p2['draws'].fillna(0)
   
    merged_p1['total_games'] = merged_p1['player1Wins'] + merged_p1['player2Wins'] + merged_p1['draws']
    merged_p2['total_games'] = merged_p2['player1Wins'] + merged_p2['player2Wins'] + merged_p2['draws']
    
    # Aggregate games won and games played by season_id and decktype for player1 decks
    p1_stats = merged_p1.groupby(['season_id', f'{decktype_column}_p1']).agg(
        games_won=('player1Wins', 'sum'),
        games_played=('total_games', 'sum')
    ).reset_index().rename(columns={f'{decktype_column}_p1': decktype_column})
    
    # Aggregate games won and games played by season_id and decktype for player2 decks
    p2_stats = merged_p2.groupby(['season_id', f'{decktype_column}_p2']).agg(
        games_won=('player2Wins', 'sum'),
        games_played=('total_games', 'sum')
    ).reset_index().rename(columns={f'{decktype_column}_p2': decktype_column})
    
    # Combine stats from both players
    combined = pd.merge(p1_stats, p2_stats, on=['season_id', decktype_column], how='outer', suffixes=('_p1', '_p2')).fillna(0)
    
    # Sum wins and games played across both player1 and player2 decks
    combined['games_won'] = combined['games_won_p1'] + combined['games_won_p2']
    combined['games_played'] = combined['games_played_p1'] + combined['games_played_p2']
    
    # Calculate winrate
    combined['game_winrate'] = combined['games_won'] / combined['games_played']
    
    return combined[['season_id', decktype_column, 'games_won', 'games_played', 'game_winrate']]

def calculate_combined_winrates_per_season(matches_df: pd.DataFrame, decks_df: pd.DataFrame) -> pd.DataFrame:
    results = []

    unique_decks = decks_df[['player', 'draft_id', 'scryfallId']].drop_duplicates()
    player_card_pairs = unique_decks[['player', 'scryfallId']].drop_duplicates()

    for _, row in player_card_pairs.iterrows():
        player = row['player']
        card = row['scryfallId']

        # Drafts where player picked this card
        player_drafts = unique_decks[
            (unique_decks['player'] == player) &
            (unique_decks['scryfallId'] == card)
        ]['draft_id'].unique()

        if len(player_drafts) == 0:
            continue

        # Matches in relevant drafts with this player
        relevant_matches = matches_df[
            (matches_df['draft_id'].isin(player_drafts)) &
            ((matches_df['player1'] == player) | (matches_df['player2'] == player))
        ]

        if relevant_matches.empty:
            continue

        for season_id, season_matches in relevant_matches.groupby('season_id'):

            matches_played = len(season_matches)

            player1_is_player = season_matches['player1'] == player
            player1_wins = season_matches['player1Wins']
            player2_wins = season_matches['player2Wins']
            draws = season_matches['draws']

            matches_won = (
                ((player1_is_player) & (player1_wins > player2_wins)) |
                (~player1_is_player & (player2_wins > player1_wins))
            ).sum()

            matches_drawn = (player1_wins == player2_wins).sum()
            matches_lost = matches_played - matches_won - matches_drawn

            games_played = (player1_wins + player2_wins + draws).sum()
            games_won = (
                season_matches.loc[player1_is_player, 'player1Wins'].sum() +
                season_matches.loc[~player1_is_player, 'player2Wins'].sum()
            )
            games_drawn = draws.sum()
            games_lost = games_played - games_won - games_drawn

            match_win_rate = matches_won / matches_played if matches_played else 0.0
            match_draw_rate = matches_drawn / matches_played if matches_played else 0.0
            game_win_rate = games_won / games_played if games_played else 0.0
            game_draw_rate = games_drawn / games_played if games_played else 0.0

            results.append({
                'player': player,
                'scryfallId': card,
                'season_id': season_id,
                'num_drafts_with_card': len(player_drafts),
                'matches_played': matches_played,
                'matches_won': matches_won,
                'matches_drawn': matches_drawn,
                'matches_lost': matches_lost,
                'match_win_rate': match_win_rate,
                'match_draw_rate': match_draw_rate,
                'games_played': games_played,
                'games_won': games_won,
                'games_drawn': games_drawn,
                'games_lost': games_lost,
                'game_win_rate': game_win_rate,
                'game_draw_rate': game_draw_rate,
            })

    return pd.DataFrame(results)

def calculate_player_archetype_game_winrate(matches_df: pd.DataFrame, decks_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate both match-level and game-level winrates per player per archetype.
    Draws are NOT counted as games played.
    Returns DataFrame with columns:
    ['season_id', 'player', 'archetype', 'matches_played', 'matches_won',
     'match_win_rate', 'games_played', 'games_won', 'game_win_rate']
    """
    player_archetypes = decks_df[['season_id', 'draft_id', 'player', 'archetype']].drop_duplicates()

    # Merge archetypes onto matches for both players
    merged = matches_df.merge(
        player_archetypes.rename(columns={'player': 'player1', 'archetype': 'archetype_p1'}),
        on=['season_id', 'draft_id', 'player1'], how='left'
    ).merge(
        player_archetypes.rename(columns={'player': 'player2', 'archetype': 'archetype_p2'}),
        on=['season_id', 'draft_id', 'player2'], how='left'
    )

    merged['total_games_per_match'] = merged['player1Wins'] + merged['player2Wins']  # Exclude draws

    # Prepare per-player match data (long format)
    p1_match = merged[['season_id', 'player1', 'archetype_p1', 'player1Wins', 'player2Wins', 'draft_id']].rename(
        columns={'player1': 'player', 'archetype_p1': 'archetype'}
    )
    p2_match = merged[['season_id', 'player2', 'archetype_p2', 'player2Wins', 'player1Wins', 'draft_id']].rename(
        columns={'player2': 'player', 'archetype_p2': 'archetype', 'player2Wins': 'player1Wins', 'player1Wins': 'player2Wins'}
    )

    match_long = pd.concat([p1_match, p2_match], ignore_index=True).dropna(subset=['archetype'])
    match_long['match_won'] = match_long['player1Wins'] > match_long['player2Wins']
    match_long['matches_played'] = 1

    # Prepare per-player game data (long format)
    p1_game = merged[['season_id', 'player1', 'archetype_p1', 'player1Wins', 'total_games_per_match']].rename(
        columns={'player1': 'player', 'archetype_p1': 'archetype', 'player1Wins': 'games_won'}
    )
    p2_game = merged[['season_id', 'player2', 'archetype_p2', 'player2Wins', 'total_games_per_match']].rename(
        columns={'player2': 'player', 'archetype_p2': 'archetype', 'player2Wins': 'games_won'}
    )
    game_long = pd.concat([p1_game, p2_game], ignore_index=True).dropna(subset=['archetype'])

    # Aggregate match stats
    match_agg = match_long.groupby(['season_id', 'player', 'archetype']).agg(
        matches_played=('matches_played', 'sum'),
        matches_won=('match_won', 'sum')
    ).reset_index()
    match_agg['match_win_rate'] = match_agg['matches_won'] / match_agg['matches_played']

    # Aggregate game stats
    game_agg = game_long.groupby(['season_id', 'player', 'archetype']).agg(
        games_played=('total_games_per_match', 'sum'),
        games_won=('games_won', 'sum')
    ).reset_index()
    game_agg['game_win_rate'] = game_agg['games_won'] / game_agg['games_played']

    # Merge match and game stats
    result = pd.merge(match_agg, game_agg, on=['season_id', 'player', 'archetype'], how='outer')

    return result[['season_id', 'player', 'archetype', 'games_won', 'games_played', 'game_win_rate', 
                   'matches_won', 'matches_played', 'match_win_rate']]


def calculate_player_decktype_game_winrate(matches_df: pd.DataFrame, decks_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate both match-level and game-level winrates per player per decktype.
    Draws are NOT counted as games played.
    
    Returns DataFrame with columns:
    ['season_id', 'player', 'decktype', 'matches_played', 'matches_won',
     'match_win_rate', 'games_played', 'games_won', 'game_win_rate']
    """
    player_decktypes = decks_df[['season_id', 'draft_id', 'player', 'decktype']].drop_duplicates()

    merged = matches_df.merge(
        player_decktypes.rename(columns={'player': 'player1', 'decktype': 'decktype_p1'}),
        on=['season_id', 'draft_id', 'player1'], how='left'
    ).merge(
        player_decktypes.rename(columns={'player': 'player2', 'decktype': 'decktype_p2'}),
        on=['season_id', 'draft_id', 'player2'], how='left'
    )

    merged['total_games_per_match'] = merged['player1Wins'] + merged['player2Wins']  # Exclude draws

    p1_match = merged[['season_id', 'player1', 'decktype_p1', 'player1Wins', 'player2Wins', 'draft_id']].rename(
        columns={'player1': 'player', 'decktype_p1': 'decktype'}
    )
    p2_match = merged[['season_id', 'player2', 'decktype_p2', 'player2Wins', 'player1Wins', 'draft_id']].rename(
        columns={'player2': 'player', 'decktype_p2': 'decktype', 'player2Wins': 'player1Wins', 'player1Wins': 'player2Wins'}
    )

    match_long = pd.concat([p1_match, p2_match], ignore_index=True).dropna(subset=['decktype'])
    match_long['match_won'] = match_long['player1Wins'] > match_long['player2Wins']
    match_long['matches_played'] = 1

    p1_game = merged[['season_id', 'player1', 'decktype_p1', 'player1Wins', 'total_games_per_match']].rename(
        columns={'player1': 'player', 'decktype_p1': 'decktype', 'player1Wins': 'games_won'}
    )
    p2_game = merged[['season_id', 'player2', 'decktype_p2', 'player2Wins', 'total_games_per_match']].rename(
        columns={'player2': 'player', 'decktype_p2': 'decktype', 'player2Wins': 'games_won'}
    )
    game_long = pd.concat([p1_game, p2_game], ignore_index=True).dropna(subset=['decktype'])

    match_agg = match_long.groupby(['season_id', 'player', 'decktype']).agg(
        matches_played=('matches_played', 'sum'),
        matches_won=('match_won', 'sum')
    ).reset_index()
    match_agg['match_win_rate'] = match_agg['matches_won'] / match_agg['matches_played']

    game_agg = game_long.groupby(['season_id', 'player', 'decktype']).agg(
        games_played=('total_games_per_match', 'sum'),
        games_won=('games_won', 'sum')
    ).reset_index()
    game_agg['game_win_rate'] = game_agg['games_won'] / game_agg['games_played']

    result = pd.merge(match_agg, game_agg, on=['season_id', 'player', 'decktype'], how='outer')

    return result[['season_id', 'player', 'decktype', 'games_won', 'games_played',  'game_win_rate', 
                   'matches_won', 'matches_played', 'match_win_rate']]



def calculate_player_game_and_match_stats(matches_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates overall game and match winrates for each player.
    Draws are NOT counted as games played.
    Returns:
        DataFrame with columns:
        ['season_id', 'player', 'games_played', 'games_won', 'games_drawn', 'game_win_rate',
         'matches_played', 'matches_won', 'match_win_rate']
    """
    matches_df = matches_df.copy()
    matches_df['draws'] = matches_df['draws'].fillna(0)
    
    # Player 1 view
    p1 = matches_df[['season_id', 'player1', 'player1Wins', 'player2Wins', 'draws']].copy()
    p1 = p1.rename(columns={'player1': 'player', 'player1Wins': 'games_won'})
    p1['games_played'] = p1['games_won'] + p1['player2Wins']  # Exclude draws
    p1['games_drawn'] = p1['draws']
    p1['matches_played'] = 1
    p1['matches_won'] = (p1['games_won'] > p1['player2Wins']).astype(int)
    
    # Player 2 view
    p2 = matches_df[['season_id', 'player2', 'player2Wins', 'player1Wins', 'draws']].copy()
    p2 = p2.rename(columns={'player2': 'player', 'player2Wins': 'games_won'})
    p2['games_played'] = p2['games_won'] + p2['player1Wins']  # Exclude draws
    p2['games_drawn'] = p2['draws']
    p2['matches_played'] = 1
    p2['matches_won'] = (p2['games_won'] > p2['player1Wins']).astype(int)
    
    # Combine both player views
    all_players = pd.concat([p1, p2], ignore_index=True)
    
    # Aggregate by player and season
    result = all_players.groupby(['season_id', 'player']).agg(
        games_played=('games_played', 'sum'),
        games_won=('games_won', 'sum'),
        games_drawn=('games_drawn', 'sum'),
        matches_played=('matches_played', 'sum'),
        matches_won=('matches_won', 'sum')
    ).reset_index()
    
    # Calculate win rates
    result['game_win_rate'] = result['games_won'] / result['games_played']
    result['match_win_rate'] = result['matches_won'] / result['matches_played']
    
    return result[['season_id', 'player', 'games_played', 'games_won', 'games_drawn', 'game_win_rate',
                   'matches_played', 'matches_won', 'match_win_rate']]

def calculate_vs_player_stats(matches_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates winrates and match counts for each player against each opponent.

    Returns:
        DataFrame with:
        ['season_id', 'player', 'opponent', 'games_played_vs', 'games_won_vs', 'game_win_rate_vs',
         'matches_played_vs', 'matches_won_vs', 'match_win_rate_vs']
    """
    matches_df = matches_df.copy()
    matches_df['draws'] = matches_df['draws'].fillna(0)

    # View from player1
    p1 = matches_df[['season_id', 'player1', 'player2', 'player1Wins', 'player2Wins', 'draws']].copy()
    p1 = p1.rename(columns={
        'player1': 'player',
        'player2': 'opponent',
        'player1Wins': 'games_won_vs',
        'player2Wins': 'games_lost_vs'
    })
    p1['games_played_vs'] = p1['games_won_vs'] + p1['games_lost_vs'] + p1['draws']
    p1['matches_played_vs'] = 1
    p1['matches_won_vs'] = (p1['games_won_vs'] > p1['games_lost_vs']).astype(int)

    # View from player2
    p2 = matches_df[['season_id', 'player2', 'player1', 'player2Wins', 'player1Wins', 'draws']].copy()
    p2 = p2.rename(columns={
        'player2': 'player',
        'player1': 'opponent',
        'player2Wins': 'games_won_vs',
        'player1Wins': 'games_lost_vs'
    })
    p2['games_played_vs'] = p2['games_won_vs'] + p2['games_lost_vs'] + p2['draws']
    p2['matches_played_vs'] = 1
    p2['matches_won_vs'] = (p2['games_won_vs'] > p2['games_lost_vs']).astype(int)

    all_vs = pd.concat([p1, p2], ignore_index=True)

    result = all_vs.groupby(['season_id', 'player', 'opponent']).agg(
        games_played_vs=('games_played_vs', 'sum'),
        games_won_vs=('games_won_vs', 'sum'),
        matches_played_vs=('matches_played_vs', 'sum'),
        matches_won_vs=('matches_won_vs', 'sum')
    ).reset_index()

    result['game_win_rate_vs'] = result['games_won_vs'] / result['games_played_vs']
    result['match_win_rate_vs'] = result['matches_won_vs'] / result['matches_played_vs']

    return result[['season_id', 'player', 'opponent',
                   'games_played_vs', 'games_won_vs', 'game_win_rate_vs',
                   'matches_played_vs', 'matches_won_vs', 'match_win_rate_vs']]
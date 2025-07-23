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


def build_card_availability_map(cube_history_df: pd.DataFrame, drafts_df: pd.DataFrame) -> dict:
    cube_history_df = cube_history_df.sort_values('timestamp').reset_index(drop=True)
    availability_map = {}

    for season, season_changes in cube_history_df.groupby('season_id'):
        season_changes = season_changes.reset_index(drop=True)
        season_drafts = drafts_df[drafts_df['season_id'] == season].sort_values('timestamp').reset_index(drop=True)

        available_cards = set()
        idx_change = 0
        availability_map[season] = {}

        for _, draft in season_drafts.iterrows():
            draft_time = draft['timestamp']
            draft_id = draft['draft_id']

            while idx_change < len(season_changes) and season_changes.loc[idx_change, 'timestamp'] <= draft_time:
                change = season_changes.loc[idx_change]
                card_id = change['scryfallId']
                if change['change_type'] == 'adds':
                    available_cards.add(card_id)
                elif change['change_type'] == 'removes':
                    available_cards.discard(card_id)
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
    card_drafts = decks_df.groupby(['season_id', 'draft_id', 'player', 'scryfallId']).size().reset_index().drop(0, axis=1)
    results = []

    card_season_pairs = card_drafts[['season_id', 'scryfallId']].drop_duplicates()

    for _, row in card_season_pairs.iterrows():
        season_id = row['season_id']
        card = row['scryfallId']

        # Drafts and players who drafted this card
        drafts_with_players = card_drafts[
            (card_drafts['season_id'] == season_id) &
            (card_drafts['scryfallId'] == card)
        ][['draft_id', 'player']]

        # Filter drafts where card was available
        drafts_with_players = drafts_with_players[
            drafts_with_players.apply(
                lambda x: season_id in card_availability_map
                          and x['draft_id'] in card_availability_map[season_id]
                          and card in card_availability_map[season_id][x['draft_id']],
                axis=1
            )
        ]

        drafts = drafts_with_players['draft_id'].unique()
        if len(drafts) == 0:
            continue

        relevant_matches = matches_df[
            (matches_df['season_id'] == season_id) &
            (matches_df['draft_id'].isin(drafts))
        ]

        if relevant_matches.empty:
            continue

        matches_played = 0
        matches_won = 0

        for _, match in relevant_matches.iterrows():
            # Players in this match
            p1 = match['player1']
            p2 = match['player2']

            # Which players drafted this card in this draft?
            players_with_card = drafts_with_players[drafts_with_players['draft_id'] == match['draft_id']]['player'].tolist()

            # Did either player draft the card?
            p1_has_card = p1 in players_with_card
            p2_has_card = p2 in players_with_card

            # Count this match only if player with card played
            if not (p1_has_card or p2_has_card):
                continue

            matches_played += 1

            # Determine winner
            if match['player1Wins'] > match['player2Wins'] and p1_has_card:
                matches_won += 1
            elif match['player2Wins'] > match['player1Wins'] and p2_has_card:
                matches_won += 1

        match_win_rate = matches_won / matches_played if matches_played > 0 else 0

        results.append({
            'season_id': season_id,
            'scryfallId': card,
            'matches_played': matches_played,
            'matches_won': matches_won,
            'match_win_rate': match_win_rate
        })

    return pd.DataFrame(results)



def calculate_card_game_winrate_per_season(matches_df: pd.DataFrame, decks_df: pd.DataFrame, card_availability_map: dict) -> pd.DataFrame:
    card_drafts = decks_df.groupby(['season_id', 'draft_id', 'scryfallId']).size().reset_index().drop(0, axis=1)
    results = []

    card_season_pairs = card_drafts[['season_id', 'scryfallId']].drop_duplicates()

    for _, row in card_season_pairs.iterrows():
        season_id = row['season_id']
        card = row['scryfallId']

        drafts_with_card = card_drafts[
            (card_drafts['season_id'] == season_id) &
            (card_drafts['scryfallId'] == card)
        ]['draft_id'].unique()

        drafts_with_card = [d for d in drafts_with_card if season_id in card_availability_map and d in card_availability_map[season_id] and card in card_availability_map[season_id][d]]

        relevant_matches = matches_df[
            (matches_df['draft_id'].isin(drafts_with_card)) &
            (matches_df['season_id'] == season_id)
        ]

        if relevant_matches.empty:
            continue

        games_played = 0
        games_won = 0

        for _, m in relevant_matches.iterrows():
            total_games = m['player1Wins'] + m['player2Wins'] + m['draws']
            games_played += total_games

            # Simplified: all wins count to decks with the card
            games_won += m['player1Wins'] + m['player2Wins']

        game_winrate = games_won / games_played if games_played > 0 else 0

        results.append({
            'season_id': season_id,
            'scryfallId': card,
            'games_played': games_played,
            'games_won': games_won,
            'game_win_rate': game_winrate
        })

    return pd.DataFrame(results)



def calculate_archetype_match_winrate(matches_df: pd.DataFrame, decks_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate match winrate per archetype (Level 2) based on match and deck data.

    Args:
        matches_df: DataFrame with match data including 'draft_id', 'player1', 'player2',
                    'player1Wins', 'player2Wins', and 'season_id'.
        decks_df: DataFrame with decks info including 'draft_id', 'player', 'archetype'.

    Returns:
        DataFrame with columns ['season_id', 'archetype', 'matches_played', 'matches_won', 'match_win_rate']
    """
    results = []

    # Merge archetype info for both players in each match
    # Join player1 archetype
    p1_decks = decks_df[['draft_id', 'player', 'archetype']].rename(
        columns={'player': 'player1', 'archetype': 'archetype_p1'})
    # Join player2 archetype
    p2_decks = decks_df[['draft_id', 'player', 'archetype']].rename(
        columns={'player': 'player2', 'archetype': 'archetype_p2'})

    matches = matches_df.merge(p1_decks, on=['draft_id', 'player1'], how='left')
    matches = matches.merge(p2_decks, on=['draft_id', 'player2'], how='left')

    # We want to calculate stats per archetype per season
    season_archetypes = set(matches['season_id'].unique())
    season_archetypes = [(season, arch) for season in matches['season_id'].unique() for arch in 
                        pd.concat([matches['archetype_p1'], matches['archetype_p2']]).dropna().unique()]

    for season_id, archetype in season_archetypes:
        # Filter matches where player1 has archetype or player2 has archetype (since match is between two players)
        filtered_matches = matches[
            (matches['season_id'] == season_id) &
            ((matches['archetype_p1'] == archetype) | (matches['archetype_p2'] == archetype))
        ]

        matches_played = len(filtered_matches)
        if matches_played == 0:
            continue

        matches_won = 0

        # Count matches won by players of the given archetype
        for _, row in filtered_matches.iterrows():
            winner = None
            if row['player1Wins'] > row['player2Wins']:
                winner = row['player1']
                winner_arch = row['archetype_p1']
            elif row['player2Wins'] > row['player1Wins']:
                winner = row['player2']
                winner_arch = row['archetype_p2']
            else:
                winner = None
                winner_arch = None

            if winner_arch == archetype:
                matches_won += 1

        match_win_rate = matches_won / matches_played if matches_played > 0 else 0

        results.append({
            'season_id': season_id,
            'archetype': archetype,
            'matches_played': matches_played,
            'matches_won': matches_won,
            'match_win_rate': match_win_rate
        })

    return pd.DataFrame(results)

def calculate_archetype_game_winrate(matches_df: pd.DataFrame, decks_df: pd.DataFrame) -> pd.DataFrame:
    results = []

    # Get all unique (season_id, archetype) pairs
    archetype_season_pairs = decks_df[['season_id', 'archetype']].drop_duplicates()

    for _, row in archetype_season_pairs.iterrows():
        season = row['season_id']
        archetype = row['archetype']

        # Get draft_ids where players played this archetype in this season
        drafts_with_archetype = decks_df[
            (decks_df['season_id'] == season) & 
            (decks_df['archetype'] == archetype)
        ]['draft_id'].unique()

        # Filter matches for these drafts and season
        relevant_matches = matches_df[
            (matches_df['season_id'] == season) &
            (matches_df['draft_id'].isin(drafts_with_archetype))
        ]

        if relevant_matches.empty:
            continue

        games_played = 0
        games_won = 0

        # For each match, we determine games played and won by this archetype
        for _, match in relevant_matches.iterrows():
            total_games = match['player1Wins'] + match['player2Wins'] + match['draws']
            games_played += total_games

            # Determine which archetype player1 and player2 had in this draft
            p1_arch = decks_df[
                (decks_df['draft_id'] == match['draft_id']) & 
                (decks_df['player'] == match['player1'])
            ]['archetype'].values
            p2_arch = decks_df[
                (decks_df['draft_id'] == match['draft_id']) & 
                (decks_df['player'] == match['player2'])
            ]['archetype'].values

            # If player1 has this archetype, add their wins to games_won
            if len(p1_arch) > 0 and p1_arch[0] == archetype:
                games_won += match['player1Wins']

            # If player2 has this archetype, add their wins to games_won
            if len(p2_arch) > 0 and p2_arch[0] == archetype:
                games_won += match['player2Wins']

        game_winrate = games_won / games_played if games_played > 0 else 0

        results.append({
            'season_id': season,
            'archetype': archetype,
            'games_played': games_played,
            'games_won': games_won,
            'game_win_rate': game_winrate
        })

    return pd.DataFrame(results)

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


def calculate_decktype_match_winrate(matches_df: pd.DataFrame, decks_df: pd.DataFrame, decktype_level: str) -> pd.DataFrame:
    """
    Calculate match winrate per decktype.

    Args:
        matches_df: DataFrame with columns ['draft_id', 'player1', 'player2', 'player1Wins', 'player2Wins', 'season_id', ...].
        decks_df: DataFrame with columns ['draft_id', 'player', decktype_level].
        decktype_level: string, column name for decktype level ('decktype ' or '').

    Returns:
        DataFrame with columns ['season_id', decktype_level, 'matches_played', 'matches_won', 'match_win_rate'].
    """
    results = []

    # Prepare decks info for both players
    p1_decks = decks_df[['draft_id', 'player', decktype_level]].rename(
        columns={'player': 'player1', decktype_level: 'decktype_p1'}
    )
    p2_decks = decks_df[['draft_id', 'player', decktype_level]].rename(
        columns={'player': 'player2', decktype_level: 'decktype_p2'}
    )

    # Merge decks info onto matches
    merged = matches_df.merge(p1_decks, on=['draft_id', 'player1'], how='left')
    merged = merged.merge(p2_decks, on=['draft_id', 'player2'], how='left')

    # Get unique (season, decktype) pairs from both players
    decktypes_p1 = merged[['season_id', 'decktype_p1']].dropna().rename(columns={'decktype_p1': decktype_level})
    decktypes_p2 = merged[['season_id', 'decktype_p2']].dropna().rename(columns={'decktype_p2': decktype_level})
    all_decktypes = pd.concat([decktypes_p1, decktypes_p2]).drop_duplicates()

    for _, row in all_decktypes.iterrows():
        season = row['season_id']
        decktype = row[decktype_level]

        # Filter matches where either player has this decktype
        relevant_matches = merged[
            (merged['season_id'] == season) &
            ((merged['decktype_p1'] == decktype) | (merged['decktype_p2'] == decktype))
        ]

        if relevant_matches.empty:
            continue

        matches_played = len(relevant_matches)
        matches_won = 0

        for _, match in relevant_matches.iterrows():
            # Determine winner decktype
            if match['player1Wins'] > match['player2Wins']:
                winner_decktype = match['decktype_p1']
            elif match['player2Wins'] > match['player1Wins']:
                winner_decktype = match['decktype_p2']
            else:
                winner_decktype = None  # Draw

            if winner_decktype == decktype:
                matches_won += 1

        match_win_rate = matches_won / matches_played if matches_played > 0 else 0

        results.append({
            'season_id': season,
            decktype_level: decktype,
            'matches_played': matches_played,
            'matches_won': matches_won,
            'match_win_rate': match_win_rate
        })

    return pd.DataFrame(results)

def calculate_decktype_game_winrate(matches_df, decks_df, decktype_column):
    """
    Calculate decktype game winrate as games won / games played.

    Parameters:
    - matches_df: DataFrame with columns including player1, player2, player1Wins, player2Wins, draft_id
    - decks_df: DataFrame with columns including draft_id, player, and the decktype_column
    - decktype_column: str, column name for the deck type level to use ('decktype ', '', etc.)

    Returns:
    - DataFrame with decktype and their game winrate.
    """

    # Merge decks info for both players in each match
    merged1 = matches_df.merge(decks_df, left_on=['draft_id', 'player1'], right_on=['draft_id', 'player'], how='left')
    merged2 = matches_df.merge(decks_df, left_on=['draft_id', 'player2'], right_on=['draft_id', 'player'], how='left')

    # Calculate games played and games won for player1 decktypes
    player1_stats = merged1.groupby(decktype_column).agg(
        games_played=('player1Wins', 'sum'),
        total_games=('player1Wins', 'count')  # count of matches for that decktype
    ).reset_index()
    # But total_games here counts matches, we want total games played = player1Wins + player2Wins for those matches

    # Instead, calculate total games played as sum(player1Wins + player2Wins) for player1 decktypes
    merged1['total_games'] = merged1['player1Wins'] + merged1['player2Wins']
    player1_stats = merged1.groupby(decktype_column).agg(
        games_won=('player1Wins', 'sum'),
        games_played=('total_games', 'sum')
    ).reset_index()

    # Similarly for player2
    merged2['total_games'] = merged2['player1Wins'] + merged2['player2Wins']
    player2_stats = merged2.groupby(decktype_column).agg(
        games_won=('player2Wins', 'sum'),
        games_played=('total_games', 'sum')
    ).reset_index()

    # Combine player1 and player2 stats by decktype
    combined_stats = pd.merge(player1_stats, player2_stats, on=decktype_column, how='outer', suffixes=('_p1', '_p2')).fillna(0)

    combined_stats['games_won_total'] = combined_stats['games_won_p1'] + combined_stats['games_won_p2']
    combined_stats['games_played_total'] = combined_stats['games_played_p1'] + combined_stats['games_played_p2']

    combined_stats['game_winrate'] = combined_stats['games_won_total'] / combined_stats['games_played_total']

    return combined_stats[[decktype_column, 'games_won_total', 'games_played_total', 'game_winrate']]

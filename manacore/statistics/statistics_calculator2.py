import os
import pandas as pd
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


def main():
    base_path = "data/processed"

    decks_df = load_drafted_decks(base_path)
    matches_df = load_match_data(base_path)
    cube_history_df = load_cube_history(base_path)
    drafts_df = load_drafts_with_timestamp(base_path)

    card_availability_map = build_card_availability_map(cube_history_df, drafts_df)

    df_mainboard = calculate_card_mainboard_rate_per_season(decks_df, drafts_df, card_availability_map)
    df_match = calculate_card_match_winrate_per_season(matches_df, decks_df, card_availability_map)
    df_game = calculate_card_game_winrate_per_season(matches_df, decks_df, card_availability_map)

    combined = df_mainboard.merge(df_match, on=['season_id', 'scryfallId'], how='outer')
    combined = combined.merge(df_game, on=['season_id', 'scryfallId'], how='outer')

    print(combined.head(50))


if __name__ == "__main__":
    main()

import os
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import pandas as pd
from manacore.config.get_seasons import load_season_config, get_season_for_date


def load_match_data(base_path: str) -> pd.DataFrame:
    matches_file = os.path.join(base_path, "matches.csv")
    if not os.path.exists(matches_file):
        raise FileNotFoundError(f"'matches.csv' not found in {base_path}")
    return pd.read_csv(matches_file)


def compute_match_stats(df: pd.DataFrame, season_config: dict) -> dict:
    player_stats = defaultdict(lambda: {
        'match_points': 0,
        'game_points': 0,
        'matches_played': 0,
        'games_played': 0,
        'opponents': set(),
        'byes': 0,
        'draft_id': None,
        'season_id': None
    })

    for _, row in df.iterrows():
        p1, p2 = row['player1'], row['player2']
        gw1, gw2, draws = row['player1Wins'], row['player2Wins'], row['draws']
        draft_id = row['draft_id']
        draft_date = datetime.strptime(str(draft_id), "%Y%m%d").date()
        season_id = get_season_for_date(draft_date, season_config)
        is_bye_p1, is_bye_p2 = row.get('player1Bye', False), row.get('player2Bye', False)
        total_games = gw1 + gw2 + draws

        if is_bye_p1:
            p1_match_points, p1_game_points, p1_games_played = 3, 6, 2
            p2_match_points, p2_game_points, p2_games_played = 0, 0, 0
        elif is_bye_p2:
            p2_match_points, p2_game_points, p2_games_played = 3, 6, 2
            p1_match_points, p1_game_points, p1_games_played = 0, 0, 0
        else:
            p1_match_points = 3 if gw1 > gw2 else 1 if gw1 == gw2 else 0
            p2_match_points = 3 if gw2 > gw1 else 1 if gw1 == gw2 else 0
            p1_game_points = gw1 * 3 + draws
            p2_game_points = gw2 * 3 + draws
            p1_games_played = p2_games_played = total_games

        p1_stats = player_stats[(p1, draft_id)]
        p1_stats['match_points'] += p1_match_points
        p1_stats['game_points'] += p1_game_points
        p1_stats['matches_played'] += 1
        p1_stats['games_played'] += p1_games_played
        if not is_bye_p1 and p2 != "BYE":
            p1_stats['opponents'].add((p2, draft_id))
        if is_bye_p1:
            p1_stats['byes'] += 1
        p1_stats['draft_id'] = draft_id
        p1_stats['season_id'] = season_id

        p2_stats = player_stats[(p2, draft_id)]
        p2_stats['match_points'] += p2_match_points
        p2_stats['game_points'] += p2_game_points
        p2_stats['matches_played'] += 1
        p2_stats['games_played'] += p2_games_played
        if not is_bye_p2 and p1 != "BYE":
            p2_stats['opponents'].add((p1, draft_id))
        if is_bye_p2:
            p2_stats['byes'] += 1
        p2_stats['draft_id'] = draft_id
        p2_stats['season_id'] = season_id

    return player_stats


def compute_tiebreaks(player_stats: dict) -> list:
    results = []

    for (player, draft_id), stats in player_stats.items():
        mwp = stats['match_points'] / (stats['matches_played'] * 3) if stats['matches_played'] else 0
        gwp = stats['game_points'] / (stats['games_played'] * 3) if stats['games_played'] else 0

        opponent_mwps, opponent_gwps = [], []
        for (opp, did) in stats['opponents']:
            opp_stats = player_stats.get((opp, did))
            if opp_stats and opp_stats['matches_played'] > 0 and opp_stats['games_played'] > 0:
                opp_mwp = max(opp_stats['match_points'] / (opp_stats['matches_played'] * 3), 0.33)
                opp_gwp = max(opp_stats['game_points'] / (opp_stats['games_played'] * 3), 0.33)
                opponent_mwps.append(opp_mwp)
                opponent_gwps.append(opp_gwp)

        omp = sum(opponent_mwps) / len(opponent_mwps) if opponent_mwps else 0
        ogp = sum(opponent_gwps) / len(opponent_gwps) if opponent_gwps else 0

        results.append({
            'season_id': stats['season_id'],
            'draft_id': draft_id,
            'player': player,
            'match_points': stats['match_points'],
            'game_points': stats['game_points'],
            'matches_played': stats['matches_played'],
            'games_played': stats['games_played'],
            'byes': stats['byes'],
            'MWP': round(mwp * 100, 4),
            'OMP': round(omp * 100, 4),
            'GWP': round(gwp * 100, 4),
            'OGP': round(ogp * 100, 4)
        })

    return results


def build_standings_dataframe(results: list) -> pd.DataFrame:
    df = pd.DataFrame(results)
    df['tiebreak_tuple'] = list(zip(df['match_points'], df['OMP'], df['GWP'], df['OGP']))

    df.sort_values(by=['season_id', 'draft_id', 'tiebreak_tuple'], ascending=[True, True, False], inplace=True)

    df['standing'] = df.groupby(['season_id', 'draft_id'])['tiebreak_tuple'] \
                       .rank(method='min', ascending=False).round().astype('Int64')

    df.drop(columns=['tiebreak_tuple'], inplace=True)

    cols = df.columns.tolist()
    cols.remove('standing')
    mp_idx = cols.index('match_points')
    cols.insert(mp_idx + 1, 'standing')
    return df[cols]


def calculate_standings(base_path="data/processed") -> pd.DataFrame:
    season_config = load_season_config()
    match_df = load_match_data(base_path)
    player_stats = compute_match_stats(match_df, season_config)
    results = compute_tiebreaks(player_stats)
    return build_standings_dataframe(results)


def save_standings_to_csv(standings_df: pd.DataFrame, output_dir: Path = Path("data/processed")):
    output_dir.mkdir(parents=True, exist_ok=True)
    standings_path = output_dir / "standings.csv"
    standings_df.to_csv(standings_path, index=False)
    print(f"Saved standings to {standings_path}")


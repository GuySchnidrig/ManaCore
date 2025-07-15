import os
import pandas as pd
from collections import defaultdict
from pathlib import Path

def calculate_standings(base_path="data/processed") -> pd.DataFrame:
    matches_file = os.path.join(base_path, "matches.csv")
    if not os.path.exists(matches_file):
        raise FileNotFoundError(f"'matches.csv' not found in {base_path}")

    df = pd.read_csv(matches_file)

    # Store stats for each player in each draft_id
    player_stats = defaultdict(lambda: {
        'match_points': 0,
        'game_points': 0,
        'matches_played': 0,
        'games_played': 0,
        'opponents': set(),
        'byes': 0,
        'draft_id': None
    })

    for _, row in df.iterrows():
        p1 = row['player1']
        p2 = row['player2']
        gw1 = row['player1Wins']
        gw2 = row['player2Wins']
        draws = row['draws']
        draft_id = row['draft_id']
        is_bye_p1 = row.get('player1Bye', False)  # You need a flag in data indicating if player got a bye
        is_bye_p2 = row.get('player2Bye', False)

        total_games = gw1 + gw2 + draws

        # Calculate match points per Appendix C:
        # Win = 3, Draw = 1, Loss = 0, Bye counts as 3 points and 6 game points, counted as 2-0 win
        if is_bye_p1:
            p1_match_points = 3
            p1_game_points = 6
            p1_games_played = 2  # bye counted as 2 games played 2-0
            p2_match_points = 0
            p2_game_points = 0
            p2_games_played = 0
            # Player 2 probably did not actually play, so no opponent added for player 1 here
        elif is_bye_p2:
            p2_match_points = 3
            p2_game_points = 6
            p2_games_played = 2
            p1_match_points = 0
            p1_game_points = 0
            p1_games_played = 0
        else:
            # Normal match
            # Match points
            if gw1 > gw2:
                p1_match_points = 3
                p2_match_points = 0
            elif gw2 > gw1:
                p1_match_points = 0
                p2_match_points = 3
            else:
                p1_match_points = 1
                p2_match_points = 1

            # Game points:
            # 3 points per game won, 1 point per draw, 0 per loss
            p1_game_points = gw1 * 3 + draws * 1
            p2_game_points = gw2 * 3 + draws * 1
            p1_games_played = total_games
            p2_games_played = total_games

        # Update player 1 stats
        p1_stats = player_stats[(p1, draft_id)]
        p1_stats['match_points'] += p1_match_points
        p1_stats['game_points'] += p1_game_points
        p1_stats['matches_played'] += 1 if not is_bye_p1 else 0  # byes do not count as played matches for opponent stats
        p1_stats['games_played'] += p1_games_played
        if not is_bye_p1 and p2 != "BYE":
            p1_stats['opponents'].add((p2, draft_id))
        if is_bye_p1:
            p1_stats['byes'] += 1
        p1_stats['draft_id'] = draft_id

        # Update player 2 stats
        p2_stats = player_stats[(p2, draft_id)]
        p2_stats['match_points'] += p2_match_points
        p2_stats['game_points'] += p2_game_points
        p2_stats['matches_played'] += 1 if not is_bye_p2 else 0
        p2_stats['games_played'] += p2_games_played
        if not is_bye_p2 and p1 != "BYE":
            p2_stats['opponents'].add((p1, draft_id))
        if is_bye_p2:
            p2_stats['byes'] += 1
        p2_stats['draft_id'] = draft_id

    results = []
    for (player, draft_id), stats in player_stats.items():
        # Calculate Match-win Percentage (MWP) with minimum 0.33
        max_match_points = stats['matches_played'] * 3
        mwp = (stats['match_points'] / max_match_points) if max_match_points > 0 else 0
        mwp = max(mwp, 0.33) if stats['matches_played'] > 0 else 0

        # Calculate Game-win Percentage (GWP) with minimum 0.33
        max_game_points = stats['games_played'] * 3
        gwp = (stats['game_points'] / max_game_points) if max_game_points > 0 else 0
        gwp = max(gwp, 0.33) if stats['games_played'] > 0 else 0

        # Opponents' match-win % average, excluding byes
        opponent_mwps = []
        opponent_gwps = []

        for (opp, did) in stats['opponents']:
            opp_stats = player_stats.get((opp, did))
            if opp_stats and opp_stats['matches_played'] > 0 and opp_stats['games_played'] > 0:
                opp_max_match_points = opp_stats['matches_played'] * 3
                opp_mwp = opp_stats['match_points'] / opp_max_match_points
                opp_mwp = max(opp_mwp, 0.33)  # minimum 0.33 per rules

                opp_max_game_points = opp_stats['games_played'] * 3
                opp_gwp_val = opp_stats['game_points'] / opp_max_game_points
                opp_gwp_val = max(opp_gwp_val, 0.33)

                opponent_mwps.append(opp_mwp)
                opponent_gwps.append(opp_gwp_val)

        omp = sum(opponent_mwps) / len(opponent_mwps) if opponent_mwps else 0
        ogp = sum(opponent_gwps) / len(opponent_gwps) if opponent_gwps else 0

        results.append({
            'player': player,
            'draft_id': draft_id,
            'match_points': stats['match_points'],
            'game_points': stats['game_points'],
            'matches_played': stats['matches_played'],
            'games_played': stats['games_played'],
            'byes': stats['byes'],
            'MWP': round(mwp*100, 4),
            'OMP': round(omp*100, 4),
            'GWP': round(gwp*100, 4),
            'OGP': round(ogp*100, 4)
        })

    standings = pd.DataFrame(results)

    # Sort by draft_id, match_points, OMP, GWP, OGP descending (higher is better)
    standings = standings.sort_values(
        by=['draft_id', 'match_points', 'OMP', 'GWP', 'OGP'],
        ascending=[True, False, False, False, False]
    )

    # Add ranking within each draft_id
    standings['standing'] = standings.groupby('draft_id').cumcount() + 1

    # Put standing after match_points
    cols = standings.columns.tolist()
    cols.remove('standing')
    mp_idx = cols.index('match_points')
    cols.insert(mp_idx + 1, 'standing')
    standings = standings[cols]

    # Final sort
    standings = standings.sort_values(by=['draft_id', 'standing'], ascending=[True, True])

    return standings

def save_standings_to_csv(standings_df: pd.DataFrame, output_dir: Path = Path("data/processed")):
    """
    Saves the standings DataFrame to a CSV file in the specified output directory.

    Args:
        standings_df (pd.DataFrame): DataFrame returned by `calculate_standings`.
        output_dir (Path): Directory where the CSV file will be saved. Default is 'data/processed'.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    standings_path = output_dir / "standings.csv"
    standings_df.to_csv(standings_path, index=False)
    print(f"Saved standings to {standings_path}")
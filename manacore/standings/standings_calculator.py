import os
import pandas as pd
from collections import defaultdict
from pathlib import Path
from datetime import datetime
from manacore.config.get_seasons import load_season_config, get_season_for_date


def calculate_standings(base_path="data/processed") -> pd.DataFrame:
    """
    Calculate player standings from match results, using match/game points and tie-breaking metrics.

    This function reads the `matches.csv` file, computes standings per draft based on:
    - Match Points (Win = 3, Draw = 1, Loss = 0, Bye = 3 points)
    - Game Points (3 per win, 1 per draw)
    - Tie-breakers including Match Win % (MWP), Opponent MWP (OMP),
      Game Win % (GWP), and Opponent GWP (OGP)

    Applies minimum thresholds of 33% for MWP and GWP when computing OMP and OGP per competitive rules.

    Parameters:
    -----------
    base_path : str, optional
        Path to the directory containing 'matches.csv'. Default is 'data/processed'.

    Returns:
    --------
    pd.DataFrame
        A DataFrame with per-draft player standings, including:
        - draft_id
        - player
        - match_points
        - standing (rank within draft)
        - games_played, matches_played, byes
        - MWP, OMP, GWP, OGP (all as percentages)

    Raises:
    -------
    FileNotFoundError
        If `matches.csv` does not exist in the specified base_path.

    Notes:
    ------
    - Standings are sorted by draft_id, then by:
        1. match_points
        2. OMP
        3. GWP
        4. OGP
    - A 'standing' column is added to represent rank within each draft.
    - Bye matches are handled as 2-0 wins (6 game points, 3 match points) but excluded from opponent stats.
    """
    season_config = load_season_config()

    matches_file = os.path.join(base_path, "matches.csv")
    if not os.path.exists(matches_file):
        raise FileNotFoundError(f"'matches.csv' not found in {base_path}")

    df = pd.read_csv(matches_file)

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
                
        p1 = row['player1']
        p2 = row['player2']
        gw1 = row['player1Wins']
        gw2 = row['player2Wins']
        draws = row['draws']
        draft_id = row['draft_id']
        
        draft_date = datetime.strptime(str(draft_id), "%Y%m%d").date()
        season_id = get_season_for_date(draft_date, season_config) if draft_date else None
        
        is_bye_p1 = row.get('player1Bye', False)  
        is_bye_p2 = row.get('player2Bye', False)

        total_games = gw1 + gw2 + draws

        # Win = 3, Draw = 1, Loss = 0, Bye counts as 3 points and 6 game points, counted as 2-0 win
        if is_bye_p1:
            p1_match_points = 3
            p1_game_points = 6
            p1_games_played = 2  
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
            if gw1 > gw2:
                p1_match_points = 3
                p2_match_points = 0
            elif gw2 > gw1:
                p1_match_points = 0
                p2_match_points = 3
            else:
                p1_match_points = 1
                p2_match_points = 1

            # 3 points per game won, 1 point per draw, 0 per loss
            p1_game_points = gw1 * 3 + draws * 1
            p2_game_points = gw2 * 3 + draws * 1
            p1_games_played = total_games
            p2_games_played = total_games

        # Update player 1 stats
        p1_stats = player_stats[(p1, draft_id)]
        p1_stats['match_points'] += p1_match_points
        p1_stats['game_points'] += p1_game_points
        p1_stats['matches_played'] += 1  # Always count the match, including byes

        p1_stats['games_played'] += p1_games_played
        if not is_bye_p1 and p2 != "BYE":
            p1_stats['opponents'].add((p2, draft_id))
        if is_bye_p1:
            p1_stats['byes'] += 1
        p1_stats['draft_id'] = draft_id
        p1_stats['season_id'] = season_id

        # Update player 2 stats
        p2_stats = player_stats[(p2, draft_id)]
        p2_stats['match_points'] += p2_match_points
        p2_stats['game_points'] += p2_game_points
        p2_stats['matches_played'] += 1  # Always count the match, including byes
        p2_stats['games_played'] += p2_games_played
        if not is_bye_p2 and p1 != "BYE":
            p2_stats['opponents'].add((p1, draft_id))
        if is_bye_p2:
            p2_stats['byes'] += 1
        p2_stats['draft_id'] = draft_id
        p2_stats['season_id'] = season_id


    results = []
    for (player, draft_id), stats in player_stats.items():
        max_match_points = stats['matches_played'] * 3
        mwp = (stats['match_points'] / max_match_points) if max_match_points > 0 else 0

        max_game_points = stats['games_played'] * 3
        gwp = (stats['game_points'] / max_game_points) if max_game_points > 0 else 0

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
                opp_gwp_val = max(opp_gwp_val, 0.33) # minimum 0.33 per rules

                opponent_mwps.append(opp_mwp)
                opponent_gwps.append(opp_gwp_val)

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
            'MWP': round(mwp*100, 4),
            'OMP': round(omp*100, 4),
            'GWP': round(gwp*100, 4),
            'OGP': round(ogp*100, 4)
        })

    standings = pd.DataFrame(results)

    # Create a "tiebreaker tuple" column for ranking
    standings['tiebreak_tuple'] = list(
        zip(
            standings['match_points'],
            standings['OMP'],
            standings['GWP'],
            standings['OGP']
        )
    )

    # Sort by tiebreakers
    standings.sort_values(
        by=['season_id', 'draft_id', 'tiebreak_tuple'],
        ascending=[True, True, False],  # descending tiebreaks
        inplace=True
    )

    # Assign rank using the tiebreak key with method='min' to group ties
    standings['standing'] = standings.groupby(['season_id', 'draft_id'])['tiebreak_tuple'] \
        .rank(method='min', ascending=False).round().astype('Int64')

    # Drop tiebreak_tuple if not needed in the final output
    standings.drop(columns=['tiebreak_tuple'], inplace=True)

    cols = standings.columns.tolist()
    cols.remove('standing')
    mp_idx = cols.index('match_points')
    cols.insert(mp_idx + 1, 'standing')
    standings = standings[cols]

    return standings

def save_standings_to_csv(standings_df: pd.DataFrame, output_dir: Path = Path("data/processed")):
    """
    Save the standings DataFrame to a CSV file in the specified output directory.

    Parameters:
    -----------
    standings_df : pd.DataFrame
        DataFrame containing player standings, typically generated by `calculate_standings`.
        Expected to include columns like ['season_id', 'player', 'wins', 'losses', 'draws', 'win_rate'], etc.

    output_dir : pathlib.Path, optional
        Directory path where the CSV will be saved. Defaults to 'data/processed'.

    Behavior:
    ---------
    - Ensures the output directory exists (creates it if necessary).
    - Writes the DataFrame to 'standings.csv' in the output directory.
    - Prints the file path upon successful save.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    standings_path = output_dir / "standings.csv"
    standings_df.to_csv(standings_path, index=False)
    print(f"Saved standings to {standings_path}")
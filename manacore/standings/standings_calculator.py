import os
import pandas as pd
from collections import defaultdict
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, Tuple, Set
from manacore.config.get_seasons import load_season_config, get_season_for_date


@dataclass
class PlayerStats:
    """Data class to hold player statistics for a draft."""
    match_points: int = 0
    game_points: int = 0
    matches_played: int = 0
    games_played: int = 0
    opponents: Set[Tuple[str, str]] = None
    byes: int = 0
    draft_id: str = None
    season_id: str = None
    
    def __post_init__(self):
        if self.opponents is None:
            self.opponents = set()


def calculate_match_points(p1_wins: int, p2_wins: int, is_bye: bool) -> Tuple[int, int]:
    """
    Calculate match points for both players based on game results.
    
    Returns:
        Tuple of (player1_match_points, player2_match_points)
    """
    if is_bye:
        return 3, 0
    
    if p1_wins > p2_wins:
        return 3, 0
    elif p2_wins > p1_wins:
        return 0, 3
    else:
        return 1, 1


def calculate_game_points(wins: int, draws: int, is_bye: bool) -> int:
    """Calculate game points for a player."""
    if is_bye:
        return 6  # Bye counts as 2-0 win
    return wins * 3 + draws * 1


def calculate_games_played(total_games: int, is_bye: bool) -> int:
    """Calculate number of games played."""
    if is_bye:
        return 2  # Bye counts as 2 games
    return total_games


def process_match_row(row: pd.Series, player_stats: Dict[Tuple[str, str], PlayerStats]) -> None:
    """
    Process a single match row and update player statistics.
    
    Args:
        row: Pandas Series containing match data
        player_stats: Dictionary to store player statistics
    """
    p1, p2 = row['player1'], row['player2']
    gw1, gw2, draws = row['player1Wins'], row['player2Wins'], row['draws']
    draft_id = row['draft_id']
    
    # Determine season
    draft_date = datetime.strptime(str(draft_id), "%Y%m%d").date()
    season_config = load_season_config()
    season_id = get_season_for_date(draft_date, season_config) if draft_date else None
    
    is_bye_p1 = row.get('player1Bye', False)
    is_bye_p2 = row.get('player2Bye', False)
    
    total_games = gw1 + gw2 + draws
    
    # Calculate points
    p1_match_points, p2_match_points = calculate_match_points(gw1, gw2, is_bye_p1)
    if is_bye_p2:
        p1_match_points, p2_match_points = 0, 3
    
    p1_game_points = calculate_game_points(gw1, draws, is_bye_p1)
    p2_game_points = calculate_game_points(gw2, draws, is_bye_p2)
    
    p1_games_played = calculate_games_played(total_games, is_bye_p1)
    p2_games_played = calculate_games_played(total_games, is_bye_p2)
    
    # Update player 1 stats
    p1_stats = player_stats[(p1, draft_id)]
    p1_stats.match_points += p1_match_points
    p1_stats.game_points += p1_game_points
    p1_stats.matches_played += 0 if is_bye_p1 else 1
    p1_stats.games_played += p1_games_played
    p1_stats.byes += 1 if is_bye_p1 else 0
    p1_stats.draft_id = draft_id
    p1_stats.season_id = season_id
    
    # Add opponent if not a bye
    if not is_bye_p1 and p2 != "BYE":
        p1_stats.opponents.add((p2, draft_id))
    
    # Update player 2 stats
    p2_stats = player_stats[(p2, draft_id)]
    p2_stats.match_points += p2_match_points
    p2_stats.game_points += p2_game_points
    p2_stats.matches_played += 0 if is_bye_p2 else 1
    p2_stats.games_played += p2_games_played
    p2_stats.byes += 1 if is_bye_p2 else 0
    p2_stats.draft_id = draft_id
    p2_stats.season_id = season_id
    
    # Add opponent if not a bye
    if not is_bye_p2 and p1 != "BYE":
        p2_stats.opponents.add((p1, draft_id))


def calculate_tiebreaker_stats(stats: PlayerStats, all_player_stats: Dict[Tuple[str, str], PlayerStats]) -> Dict[str, float]:
    """
    Calculate tiebreaker statistics (MWP, OMP, GWP, OGP) for a player.
    
    Args:
        stats: Player statistics
        all_player_stats: All player statistics for opponent lookup
        
    Returns:
        Dictionary with tiebreaker percentages
    """
    # Match Win Percentage
    max_match_points = stats.matches_played * 3
    mwp = (stats.match_points / max_match_points) if max_match_points > 0 else 0
    
    # Game Win Percentage
    max_game_points = stats.games_played * 3
    gwp = (stats.game_points / max_game_points) if max_game_points > 0 else 0
    
    # Opponent statistics
    opponent_mwps = []
    opponent_gwps = []
    
    for opp_key in stats.opponents:
        opp_stats = all_player_stats.get(opp_key)
        if not opp_stats or opp_stats.matches_played == 0 or opp_stats.games_played == 0:
            continue
            
        # Opponent Match Win Percentage (minimum 33%)
        opp_max_match_points = opp_stats.matches_played * 3
        omp_val = max(opp_stats.match_points / opp_max_match_points, 0.33)
        opponent_mwps.append(omp_val)
        
        # Opponent Game Win Percentage (minimum 33%)
        opp_max_game_points = opp_stats.games_played * 3
        ogp_val = max(opp_stats.game_points / opp_max_game_points, 0.33)
        opponent_gwps.append(ogp_val)
    
    omp = sum(opponent_mwps) / len(opponent_mwps) if opponent_mwps else 0
    ogp = sum(opponent_gwps) / len(opponent_gwps) if opponent_gwps else 0
    
    return {
        'MWP': round(mwp * 100, 4),
        'OMP': round(omp * 100, 4),
        'GWP': round(gwp * 100, 4),
        'OGP': round(ogp * 100, 4)
    }


def calculate_standings(base_path: str = "data/processed") -> pd.DataFrame:
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
        - draft_id, player, match_points, standing (rank within draft)
        - games_played, matches_played, byes
        - MWP, OMP, GWP, OGP (all as percentages)

    Raises:
    -------
    FileNotFoundError
        If `matches.csv` does not exist in the specified base_path.
    """
    matches_file = Path(base_path) / "matches.csv"
    if not matches_file.exists():
        raise FileNotFoundError(f"'matches.csv' not found in {base_path}")

    df = pd.read_csv(matches_file)
    
    # Validate required columns
    required_columns = ['player1', 'player2', 'player1Wins', 'player2Wins', 'draws', 'draft_id']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # Initialize player stats using defaultdict with PlayerStats factory
    player_stats = defaultdict(PlayerStats)

    # Process each match
    for _, row in df.iterrows():
        process_match_row(row, player_stats)

    # Calculate final standings
    results = []
    for (player, draft_id), stats in player_stats.items():
        tiebreaker_stats = calculate_tiebreaker_stats(stats, player_stats)
        
        result = {
            'season_id': stats.season_id,
            'draft_id': draft_id,
            'player': player,
            'match_points': stats.match_points,
            'game_points': stats.game_points,
            'matches_played': stats.matches_played,
            'games_played': stats.games_played,
            'byes': stats.byes,
            **tiebreaker_stats
        }
        results.append(result)

    standings = pd.DataFrame(results)
    
    if standings.empty:
        return standings

    # Sort and rank
    standings = standings.sort_values(
        by=['season_id', 'draft_id', 'match_points', 'OMP', 'GWP', 'OGP'],
        ascending=[True, True, False, False, False, False]
    )

    # Assign standings within each draft
    standings['standing'] = standings.groupby(['season_id', 'draft_id']).cumcount() + 1

    # Reorder columns
    column_order = [
        'season_id', 'draft_id', 'player', 'match_points', 'standing',
        'game_points', 'matches_played', 'games_played', 'byes',
        'MWP', 'OMP', 'GWP', 'OGP'
    ]
    standings = standings[column_order]

    return standings


def save_standings_to_csv(standings_df: pd.DataFrame, output_dir: Path = Path("data/processed")) -> None:
    """
    Save the standings DataFrame to a CSV file in the specified output directory.

    Parameters:
    -----------
    standings_df : pd.DataFrame
        DataFrame containing player standings
    output_dir : pathlib.Path, optional
        Directory path where the CSV will be saved. Defaults to 'data/processed'.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    standings_path = output_dir / "standings.csv"
    standings_df.to_csv(standings_path, index=False)
    print(f"Saved standings to {standings_path}")


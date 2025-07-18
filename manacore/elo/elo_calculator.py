
import csv
import datetime
from collections import defaultdict
from datetime import datetime
from manacore.config.get_seasons import load_season_config,get_season_for_date


def load_latest_elos(elo_history_file):
    """
    Load the latest Elo ratings for players from an Elo history CSV file.

    Parameters:
    -----------
    elo_history_file : str
        Path to a CSV file containing Elo history data with columns:
        - 'draft_id' (e.g. 'S1D5', 'S4D...' etc.)
        - 'player_name'
        - 'elo' (float rating)

    Returns:
    --------
    collections.defaultdict
        A defaultdict mapping player names to their latest Elo rating (float).
        Players not found in the file will default to an Elo rating of 1000.

    Behavior:
    ---------
    - Ignores drafts where draft_id starts with 'S4D'.
    - For each player, sorts their drafts chronologically by season and draft number.
    - Returns the Elo from the most recent draft for each player.
    """
    player_drafts = defaultdict(list)

    with open(elo_history_file, newline='') as file:
        reader = csv.DictReader(file)
        for row in reader:
            draft_id = row['draft_id']

            # Exclude drafts starting with 'S4D'
            if draft_id.startswith('S4D'):
                continue

            player = row['player_name']
            elo = float(row['elo'])

            player_drafts[player].append((draft_id, elo))

    latest_elos = {}

    # Sort drafts per player by draft_id (season and draft number)
    for player, drafts in player_drafts.items():
        def sort_key(draft):
            draft_id = draft[0]  # e.g. 'S1D5'
            season_num = int(draft_id[1:draft_id.index('D')])
            draft_num = int(draft_id[draft_id.index('D')+1:])
            return (season_num, draft_num)

        sorted_drafts = sorted(drafts, key=sort_key)
        latest_elos[player] = sorted_drafts[-1][1]  # Elo value

    return defaultdict(lambda: 1000, latest_elos)

def expected_score(rating_a, rating_b):
    """
    Calculate the expected probability that Player A wins against Player B based on Elo ratings.

    Parameters:
    -----------
    rating_a : float
        Elo rating of Player A.
    rating_b : float
        Elo rating of Player B.

    Returns:
    --------
    float
        Expected score (probability) of Player A winning, a value between 0 and 1.
    """
    return 1 / (1 + 10 ** ((rating_b - rating_a) / 400))

def update_elo(rating_a, rating_b, score_a):
    """
    Update the Elo ratings of two players after a match.

    Parameters:
    -----------
    rating_a : float
        Current Elo rating of Player A.
    rating_b : float
        Current Elo rating of Player B.
    score_a : float
        Actual score of Player A in the match, based on result:
        - 1.0 for a dominant win (e.g., 2-0),
        - 0.67 for a narrow win (e.g., 2-1),
        - 0.5 for a draw,
        - 0.33 for a narrow loss (e.g., 1-2),
        - 0.0 for a clear loss (e.g., 0-2).

    Returns:
    --------
    tuple (float, float)
        Updated Elo ratings for Player A and Player B, respectively.
    """
    K = 32
    expected_a = expected_score(rating_a, rating_b)
    expected_b = expected_score(rating_b, rating_a)

    rating_a += K * (score_a - expected_a)
    rating_b += K * ((1 - score_a) - expected_b)

    return rating_a, rating_b

def process_matches(csv_file, output_file):
    """
    Processes match data from a CSV file to update player Elo ratings across drafts and seasons,
    and writes the Elo progress of all players to an output CSV file, including matches played per draft.

    Parameters:
    -----------
    csv_file : str
        Path to the input CSV file containing match records. Each row should have fields:
        - 'draft_id' (string date like "YYYYMMDD")
        - 'player1', 'player2' (player names)
        - 'player1Wins', 'player2Wins', 'draws' (match outcomes)

    output_file : str
        Path to the output CSV file where Elo rating progress will be saved.
        The output CSV will have columns: ['season_id', 'draft_id', 'player_name', 'matches_played', 'elo'].

    Behavior:
    ---------
    - Loads season configuration and initial player Elo ratings.
    - Iterates over match records, grouped by drafts, updating Elo ratings after each match.
    - Tracks number of matches played by each player per draft.
    - Writes all Elo progress entries, including matches played, to the output CSV file.
    """
    season_config = load_season_config()
    initial_ratings = load_latest_elos("data/raw/player_elo.csv")
    ratings = initial_ratings.copy()
    elo_progress = []

    current_draft = None
    draft_players = set()
    all_players = set(ratings.keys())
    last_season = None
    last_elo_by_player = ratings.copy()
    
    from collections import defaultdict
    matches_played_per_draft = defaultdict(int)

    with open(csv_file, newline='') as file:
        reader = list(csv.DictReader(file))

        for i, row in enumerate(reader):
            draft_id = row['draft_id']  # string like "20250101"
            
            try:
                draft_date = datetime.strptime(draft_id, "%Y%m%d").date()
            except Exception:
                draft_date = None
            
            season_id = get_season_for_date(draft_date, season_config) if draft_date else None
            
            if not season_id:
                season_id = last_season
            else:
                last_season = season_id

            p1 = row['player1']
            p2 = row['player2']
            p1wins = int(row['player1Wins'])
            p2wins = int(row['player2Wins'])
            draws = int(row['draws'])

            # New draft started
            if draft_id != current_draft:
                if current_draft is not None:
                    # Fill in Elo for non-participants, with last known season & matches played
                    for player in all_players:
                        if player not in draft_players:
                            matches_played = matches_played_per_draft.get((current_draft, player), 0)
                            elo_progress.append((
                                last_season or "Unknown Season",
                                current_draft,
                                player,
                                matches_played,
                                last_elo_by_player.get(player, ratings.get(player, 1500))
                                
                            ))
                # Reset for new draft
                current_draft = draft_id
                draft_players = set()

            draft_players.update([p1, p2])
            all_players.update([p1, p2])

            # Update matches played count for both players this draft
            matches_played_per_draft[(draft_id, p1)] += 1
            matches_played_per_draft[(draft_id, p2)] += 1

            total_games = p1wins + p2wins + draws
            if p1wins == 2 and p2wins == 0:
                score_p1 = 1.0
            elif p1wins == 2 and p2wins == 1:
                score_p1 = 0.67
            elif p2wins == 2 and p1wins == 0:
                score_p1 = 0.0
            elif p2wins == 2 and p1wins == 1:
                score_p1 = 0.33
            else:
                score_p1 = (p1wins + 0.5 * draws) / total_games if total_games > 0 else 0.5

            r1, r2 = ratings.get(p1, 1000), ratings.get(p2, 1000)
            new_r1, new_r2 = update_elo(r1, r2, score_p1)
            ratings[p1], ratings[p2] = new_r1, new_r2

            last_elo_by_player[p1] = new_r1
            last_elo_by_player[p2] = new_r2

            elo_progress.append((
                season_id,
                draft_id,
                p1,
                matches_played_per_draft[(draft_id, p1)],
                new_r1
            ))
            elo_progress.append((
                season_id,
                draft_id,
                p2,
                matches_played_per_draft[(draft_id, p2)],
                new_r2
            ))

        # Handle last draft’s non-participants after all rows
        for player in all_players:
            if player not in draft_players:
                matches_played = matches_played_per_draft.get((current_draft, player), 0)
                elo_progress.append((
                    last_season or "Unknown Season",
                    current_draft,
                    player,
                    matches_played,
                    last_elo_by_player.get(player, ratings.get(player, 1000))

                ))

    # Write output CSV with matches_played column
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['season_id', 'draft_id', 'player', 'match_id', 'elo'])
        for row in elo_progress:
            writer.writerow(row)

    print(f"Wrote {len(elo_progress)} rows to {output_file}")
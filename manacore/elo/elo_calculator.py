
import csv
import datetime
from collections import defaultdict
from datetime import datetime
from manacore.config.get_seasons import load_season_config,get_season_for_date


def load_latest_elos(elo_history_file):
    """
    Load the latest Elo ratings for players from a wide-format Elo history CSV file.

    Parameters:
    -----------
    elo_history_file : str
        Path to a CSV file with columns: 'player', 'baseElo', and draft columns like 'S1D1', ..., 'S3D12'.

    Returns:
    --------
    collections.defaultdict
        A defaultdict mapping player names to their latest Elo rating (float).
        Defaults to 1000 if the player is not found.
    """
    latest_elos = {}

    with open(elo_history_file, newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        # Clean up BOM if present
        reader.fieldnames = [field.lstrip('\ufeff').strip() for field in reader.fieldnames]
        
        draft_columns = [col for col in reader.fieldnames if col.startswith('S')]

        for row in reader:
            player = row['player'].strip()

            draft_elo_values = [row[col] for col in draft_columns if row[col]]

            if draft_elo_values:
                latest_elo = float(draft_elo_values[-1])
            else:
                latest_elo = float(row.get('baseElo', 1000))

            latest_elos[player] = latest_elo

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
        - 1.0 for a narrow win (e.g., 2-1),
        - 0.5 for a draw,
        - 0.0 for a narrow loss (e.g., 1-2),
        - 0.0 for a clear loss (e.g., 0-2).

    Returns:
    --------
    tuple (float, float)
        Updated Elo ratings for Player A and Player B, respectively.
    """
    K = 32
    expected_a = expected_score(rating_a, rating_b)
    expected_b = 1 - expected_a

    score_b = 1 - score_a

    rating_a += K * (score_a - expected_a)
    rating_b += K * (score_b - expected_b)

    return rating_a, rating_b

import csv
from datetime import datetime
from collections import defaultdict

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
    """
    season_config = load_season_config()
    ratings = load_latest_elos("data/raw/elo_history.csv")
    elo_progress = []

    current_draft = None
    draft_players = set()
    all_players = set(ratings.keys())
    last_season = None
    last_elo_by_player = ratings.copy()

    matches_played_per_draft = defaultdict(int)

    with open(csv_file, newline='') as file:
        reader = list(csv.DictReader(file))

        for i, row in enumerate(reader):
            draft_id = row['draft_id']

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

            if draft_id != current_draft:
                if current_draft is not None:
                    for player in all_players:
                        if player not in draft_players:
                            matches_played = matches_played_per_draft.get((current_draft, player), 0)
                            elo_progress.append((
                                last_season or "Unknown Season",
                                current_draft,
                                player,
                                matches_played,
                                last_elo_by_player.get(player, 1000),
                                0  # No rating change for non-participants
                            ))
                current_draft = draft_id
                draft_players = set()

            draft_players.update([p1, p2])
            all_players.update([p1, p2])

            matches_played_per_draft[(draft_id, p1)] += 1
            matches_played_per_draft[(draft_id, p2)] += 1

            # Assign base full scores for update_elo and modifier
            if p1wins == 2 and p2wins == 0:
                score_p1 = 1.0
                modifier = 1.0

            elif p1wins == 2 and p2wins == 1:
                score_p1 = 1.0
                modifier = 0.67

            elif p2wins == 2 and p1wins == 0:
                score_p1 = 0.0
                modifier = 1.0

            elif p2wins == 2 and p1wins == 1:
                score_p1 = 0.0
                modifier = 0.67 
            else:
                total_games = p1wins + p2wins + draws
                if total_games > 0:
                    score_p1 = (p1wins + 0.5 * draws) / total_games
                else:
                    score_p1 = 0.5
                modifier_p1 = 1.0
                modifier_p2 = 1.0

            # Get current ratings
            r1 = ratings.get(p1, 1000)
            r2 = ratings.get(p2, 1000)

            # Update ratings with full score (no modifier here)
            new_r1, new_r2 = update_elo(r1, r2, score_p1)
            
            # Calculate raw rating changes
            change_p1 = new_r1 - r1
            change_p2 = new_r2 - r2

            # Apply modifier to rating changes
            scaled_change_p1 = change_p1 * modifier
            scaled_change_p2 = change_p2 * modifier

            # Final ratings after modifier applied
            final_r1 = r1 + scaled_change_p1
            final_r2 = r2 + scaled_change_p2
            
            print(r1, new_r1, modifier, scaled_change_p1, final_r1)
            
            # Store updated ratings
            ratings[p1] = final_r1
            ratings[p2] = final_r2
            last_elo_by_player[p1] = final_r1
            last_elo_by_player[p2] = final_r2
                    
            # Record elo progress
            elo_progress.append((
                season_id,
                draft_id,
                p1,
                matches_played_per_draft[(draft_id, p1)],
                final_r1,
                scaled_change_p1
            ))
            elo_progress.append((
                season_id,
                draft_id,
                p2,
                matches_played_per_draft[(draft_id, p2)],
                final_r2,
                scaled_change_p2
            ))

        for player in all_players:
            if player not in draft_players:
                matches_played = matches_played_per_draft.get((current_draft, player), 0)
                elo_progress.append((
                    last_season or "Unknown Season",
                    current_draft,
                    player,
                    matches_played,
                    last_elo_by_player.get(player, 1000),
                    0  # No rating change for non-participants
                ))

    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        # Add rating_change to header
        writer.writerow(['season_id', 'draft_id', 'player_name', 'matches_played', 'elo', 'rating_change'])
        for row in elo_progress:
            writer.writerow(row)

    print(f"Wrote {len(elo_progress)} rows to {output_file}")
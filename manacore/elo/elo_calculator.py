
import csv
import json
import datetime
from collections import defaultdict
from datetime import datetime
from manacore.config.get_seasons import load_season_config, get_season_for_date
from typing import Tuple


def get_k_factor(config_path="manacore/elo/elo_config.json") -> int:
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    return config["elo"]["k_factor"]

def get_default_elo(config_path="manacore/elo/elo_config.json") -> int:
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    return config["elo"]["default"]


def load_latest_elos(elo_history_file: str) -> defaultdict:
    """
    Load the latest Elo ratings for players from a wide-format Elo history CSV file.
    """
    DEFAULT_ELO = get_default_elo()
    latest_elos = {}

    with open(elo_history_file, newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        reader.fieldnames = [field.lstrip('\ufeff').strip() for field in reader.fieldnames]
        draft_columns = [col for col in reader.fieldnames if col.startswith('S')]

        for row in reader:
            player = row['player'].strip()
            draft_elo_values = [row[col] for col in draft_columns if row[col]]

            latest_elo = float(draft_elo_values[-1]) if draft_elo_values else float(row.get('baseElo', DEFAULT_ELO))
            latest_elos[player] = latest_elo

    return defaultdict(lambda: DEFAULT_ELO, latest_elos)


def expected_score(rating_a: float, rating_b: float) -> float:
    """Calculate the expected probability that Player A wins against Player B."""
    return 1 / (1 + 10 ** ((rating_b - rating_a) / 400))


def update_elo(rating_a: float, rating_b: float, score_a: float) -> Tuple[float, float]:
    """Update Elo ratings of two players after a match."""
    K_FACTOR = get_k_factor()
    expected_a = expected_score(rating_a, rating_b)
    score_b = 1 - score_a

    rating_a += K_FACTOR * (score_a - expected_a)
    rating_b += K_FACTOR * (score_b - (1 - expected_a))
    return rating_a, rating_b


def determine_score_and_modifier(p1wins: int, p2wins: int, draws: int) -> Tuple[float, float]:
    """Determine score and modifier based on match result."""
    if p1wins == 2 and p2wins == 0:
        return 1.0, 1.0
    if p1wins == 2 and p2wins == 1:
        return 1.0, 0.67
    if p2wins == 2 and p1wins == 0:
        return 0.0, 1.0
    if p2wins == 2 and p1wins == 1:
        return 0.0, 0.67

    total_games = p1wins + p2wins + draws
    score = (p1wins + 0.5 * draws) / total_games if total_games > 0 else 0.5
    return score, 1.0


def append_inactive_players_progress(
    elo_progress, all_players, draft_players, draft_id, season_id,
    last_elo_by_player, matches_played_per_draft
):
    """Log Elo for players who didn't play in the current draft."""
    DEFAULT_ELO = get_default_elo()
    
    for player in sorted(all_players):
        if player not in draft_players:
            matches_played = matches_played_per_draft.get((draft_id, player), 0)
            elo_progress.append((
                season_id or "Unknown Season",
                draft_id,
                player,
                matches_played,
                last_elo_by_player.get(player, DEFAULT_ELO),
                0  # No rating change
            ))

def process_matches(csv_file: str, output_file: str):
    """
    Process match data to update Elo ratings and write Elo history to a CSV file.
    """
    season_config = load_season_config()
    ratings = load_latest_elos("data/raw/elo_history.csv")
    elo_progress = []
    DEFAULT_ELO = get_default_elo()

    current_draft = None
    draft_players = set()
    all_players = set(ratings.keys())
    last_season = None
    last_elo_by_player = ratings.copy()
    matches_played_per_draft = defaultdict(int)

    with open(csv_file, newline='') as file:
        reader = list(csv.DictReader(file))

        for row in reader:
            draft_id = row['draft_id']
            p1, p2 = row['player1'], row['player2']
            p1wins, p2wins, draws = int(row['player1Wins']), int(row['player2Wins']), int(row['draws'])

            try:
                draft_date = datetime.strptime(draft_id, "%Y%m%d").date()
                season_id = get_season_for_date(draft_date, season_config)
                last_season = season_id
            except Exception:
                season_id = last_season

            if draft_id != current_draft:
                if current_draft is not None:
                    append_inactive_players_progress(
                        elo_progress, all_players, draft_players, current_draft, last_season, last_elo_by_player, matches_played_per_draft
                    )
                current_draft = draft_id
                draft_players = set()

            draft_players.update([p1, p2])
            all_players.update([p1, p2])

            matches_played_per_draft[(draft_id, p1)] += 1
            matches_played_per_draft[(draft_id, p2)] += 1

            score_p1, modifier = determine_score_and_modifier(p1wins, p2wins, draws)

            r1, r2 = ratings.get(p1, DEFAULT_ELO), ratings.get(p2, DEFAULT_ELO)
            new_r1, new_r2 = update_elo(r1, r2, score_p1)

            scaled_change_p1 = (new_r1 - r1) * modifier
            scaled_change_p2 = (new_r2 - r2) * modifier

            if "Missing Player" in (p1, p2):
                scaled_change_p1 = scaled_change_p2 = 0
                final_r1, final_r2 = r1, r2
            else:
                final_r1 = r1 + scaled_change_p1
                final_r2 = r2 + scaled_change_p2

            ratings[p1], ratings[p2] = final_r1, final_r2
            last_elo_by_player[p1], last_elo_by_player[p2] = final_r1, final_r2

            elo_progress.extend([
                (season_id, draft_id, p1, matches_played_per_draft[(draft_id, p1)], final_r1, scaled_change_p1),
                (season_id, draft_id, p2, matches_played_per_draft[(draft_id, p2)], final_r2, scaled_change_p2),
            ])

        append_inactive_players_progress(
            elo_progress, all_players, draft_players, current_draft, last_season, last_elo_by_player, matches_played_per_draft
        )

    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['season_id', 'draft_id', 'player_name', 'matches_played', 'elo', 'rating_change'])
        writer.writerows(elo_progress)

    print(f"Wrote {len(elo_progress)} rows to {output_file}")

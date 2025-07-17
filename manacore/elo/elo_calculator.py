import math
import csv
from collections import defaultdict

def load_latest_elos(elo_history_file):
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
    return 1 / (1 + 10 ** ((rating_b - rating_a) / 400))

def update_elo(rating_a, rating_b, score_a):
    K= 32
    expected_a = expected_score(rating_a, rating_b)
    expected_b = expected_score(rating_b, rating_a)
    
    rating_a += K * (score_a - expected_a)
    rating_b += K * ((1 - score_a) - expected_b)
    
    return rating_a, rating_b

def process_matches(csv_file):
    initial_ratings = load_latest_elos("data/raw/player_elo.csv")
    ratings = initial_ratings.copy()
    
    with open(csv_file, newline='') as file:
        reader = csv.DictReader(file)
        for row in reader:
            p1 = row['player1']
            p2 = row['player2']
            p1wins = int(row['player1Wins'])
            p2wins = int(row['player2Wins'])
            draws = int(row['draws'])

            total_games = p1wins + p2wins + draws
            score_p1 = (p1wins + 0.5 * draws) / total_games
            score_p2 = 1 - score_p1

            r1, r2 = ratings[p1], ratings[p2]
            new_r1, new_r2 = update_elo(r1, r2, score_p1)
            ratings[p1], ratings[p2] = new_r1, new_r2

    return ratings



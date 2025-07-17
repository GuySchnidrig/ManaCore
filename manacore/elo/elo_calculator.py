import math
import csv
from collections import defaultdict

# Initial ratings (can load from file or previous system)
initial_ratings = defaultdict(lambda: 1500)  # Default start if not found


def expected_score(rating_a, rating_b):
    return 1 / (1 + 10 ** ((rating_b - rating_a) / 400))

def update_elo(rating_a, rating_b, score_a):
    expected_a = expected_score(rating_a, rating_b)
    expected_b = expected_score(rating_b, rating_a)
    
    rating_a += K * (score_a - expected_a)
    rating_b += K * ((1 - score_a) - expected_b)
    
    return rating_a, rating_b

# Load match data
def process_matches(csv_file):
    K= 32
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



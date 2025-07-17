from manacore.elo.elo_calculator import process_matches, expected_score, update_elo, load_latest_elos

# Run the Elo processing on the match file
file_path = 'data/processed/matches.csv'

final_ratings = process_matches(file_path)

# Print the final Elo ratings
print("Final Elo Ratings:")
for player, rating in sorted(final_ratings.items(), key=lambda x: x[1], reverse=True):
    print(f"{player:15} {rating:.2f}")
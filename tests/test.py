def expected_score(rating_a, rating_b):
    return 1 / (1 + 10 ** ((rating_b - rating_a) / 400))

def update_elo(rating_a, rating_b, score_a):
    K = 32
    expected_a = expected_score(rating_a, rating_b)
    expected_b = 1 - expected_a

    score_b = 1 - score_a

    rating_a += K * (score_a - expected_a)
    rating_b += K * (score_b - expected_b)

    return rating_a, rating_b

# Starting ratings
r1, r2 = 1166.4, 1083.2

# Dominant 2-0 win score
score_a = 1.0

# Modifier for dominant win is 1.0 (full change)
modifier = 1

# Calculate expected score
expected_a = expected_score(r1, r2)

# Update ratings with full score
new_r1, new_r2 = update_elo(r1, r2, score_a)

# Calculate raw rating changes
change_p1 = new_r1 - r1
change_p2 = new_r2 - r2

# Apply modifier to rating changes
scaled_change_p1 = change_p1 * modifier
scaled_change_p2 = change_p2 * modifier

# Final ratings after modifier applied
final_r1 = r1 + scaled_change_p1
final_r2 = r2 + scaled_change_p2

print(
    f"Initial Ratings: A={r1}, B={r2}\n"
    f"Score A: {score_a}, Expected A: {expected_a:.4f}\n"
    f"Raw Rating Changes: A={change_p1:.2f}, B={change_p2:.2f}\n"
    f"Modifier Applied: {modifier}\n"
    f"Scaled Rating Changes: A={scaled_change_p1:.2f}, B={scaled_change_p2:.2f}\n"
    f"Final Ratings: A={final_r1:.2f}, B={final_r2:.2f}"
)

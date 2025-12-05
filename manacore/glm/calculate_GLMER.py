import os
import sys
import json
import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings('ignore')

# Add project root to sys.path
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if root_path not in sys.path:
    sys.path.insert(0, root_path)


# -------------------------------------------------------------------
# Load input CSVs
# -------------------------------------------------------------------

def load_data(processed_dir: str):
    if not os.path.exists(processed_dir):
        raise FileNotFoundError(f"Directory not found: {processed_dir}")

    matches_df = pd.read_csv(os.path.join(processed_dir, "matches.csv"))
    decks_df = pd.read_csv(os.path.join(processed_dir, "drafted_decks.csv"))
    standings_df = pd.read_csv(os.path.join(processed_dir, "standings.csv"))

    print(f"Loaded {len(matches_df)} matches, {len(decks_df)} deck-card rows")
    return matches_df, decks_df, standings_df


# -------------------------------------------------------------------
# Player ELO extraction
# -------------------------------------------------------------------

def get_player_ratings(processed_dir: str, standings_df: pd.DataFrame):
    elo_df = pd.read_csv(os.path.join(processed_dir, "elo_development.csv"))
    player_elo = (
        elo_df.sort_values("draft_id")
        .groupby("player")
        .tail(1)[["player", "elo"]]
    )
    player_elo.columns = ["player_name", "player_elo"]

    print(
        f"ELO range: {player_elo['player_elo'].min():.0f}–{player_elo['player_elo'].max():.0f}"
    )
    return player_elo


# -------------------------------------------------------------------
# Combine matches + decks → add card lists
# -------------------------------------------------------------------

def prepare_data_for_r(processed_dir: str):
    matches_df, decks_df, standings_df = load_data(processed_dir)
    player_elo = get_player_ratings(processed_dir, standings_df)

    # Unique deck structure
    unique_decks = (
        decks_df.groupby("deck_id")
        .agg({
            "player": "first",
            "archetype": "first",
            "decktype": "first",
            "draft_id": "first",
            "season_id": "first",
            "deck_color_short": "first",
        })
        .reset_index()
    )

    # Merge for P1 deck
    data = matches_df.merge(
        unique_decks,
        left_on=["draft_id", "player1"],
        right_on=["draft_id", "player"],
        how="left",
    ).drop("player", axis=1).rename(
        columns={"deck_id": "deck_id_p1", "archetype": "archetype_p1", "deck_color_short": "color_p1"}
    )

    # Merge for P2 deck
    data = data.merge(
        unique_decks,
        left_on=["draft_id", "player2"],
        right_on=["draft_id", "player"],
        how="left",
    ).drop("player", axis=1).rename(
        columns={"deck_id": "deck_id_p2", "archetype": "archetype_p2", "deck_color_short": "color_p2"}
    )

    # Add ELOs
    data = data.merge(
        player_elo, left_on="player1", right_on="player_name", how="left"
    ).drop("player_name", axis=1).rename(columns={"player_elo": "player_elo_p1"})

    data = data.merge(
        player_elo, left_on="player2", right_on="player_name", how="left"
    ).drop("player_name", axis=1).rename(columns={"player_elo": "player_elo_p2"})

    # Add card lists
    deck_cards = decks_df.groupby("deck_id")["scryfallId"].apply(list).reset_index()
    deck_cards.columns = ["deck_id", "cards"]

    data = data.merge(deck_cards, left_on="deck_id_p1", right_on="deck_id", how="left") \
               .drop("deck_id", axis=1).rename(columns={"cards": "cards_p1"})

    data = data.merge(deck_cards, left_on="deck_id_p2", right_on="deck_id", how="left") \
               .drop("deck_id", axis=1).rename(columns={"cards": "cards_p2"})

    print(f"Prepared {len(data)} matches with deck + ELO info")
    return data


# -------------------------------------------------------------------
# Build game-level dataset (FAST)
# -------------------------------------------------------------------

def create_game_level_data_fast(matches_df):
    rows = []

    valid = matches_df.dropna(subset=[
        "deck_id_p1", "deck_id_p2",
        "archetype_p1", "archetype_p2",
        "color_p1", "color_p2",
    ])

    print(f"Building game-level rows from {len(valid)} matches...")

    for idx, m in valid.iterrows():
        if idx % 200 == 0:
            print(f"  {idx}/{len(valid)} processed")

        # Helper to append one row
        def add_row(win, p, o, p_elo, o_elo, arch, arch_opp, color, deck, cards):
            rows.append({
                "win": win,
                "player_name": p,
                "opponent_name": o,
                "player_elo": p_elo,
                "opponent_elo": o_elo,
                "elo_diff": p_elo - o_elo,
                "elo_mean": (p_elo + o_elo) / 2,
                "archetype": arch,
                "archetype_opponent": arch_opp,
                "color": color,
                "deck_id": deck,
                "draft_id": m["draft_id"],
                # IMPORTANT: valid JSON for R
                "cards": json.dumps(cards if isinstance(cards, list) else [])
            })

        # P1 wins
        for _ in range(int(m["player1Wins"])):
            add_row(1, m["player1"], m["player2"],
                    m["player_elo_p1"], m["player_elo_p2"],
                    m["archetype_p1"], m["archetype_p2"],
                    m["color_p1"], m["deck_id_p1"], m["cards_p1"])

        # P1 losses == P2 wins
        for _ in range(int(m["player2Wins"])):
            add_row(0, m["player1"], m["player2"],
                    m["player_elo_p1"], m["player_elo_p2"],
                    m["archetype_p1"], m["archetype_p2"],
                    m["color_p1"], m["deck_id_p1"], m["cards_p1"])

        # P2 wins
        for _ in range(int(m["player2Wins"])):
            add_row(1, m["player2"], m["player1"],
                    m["player_elo_p2"], m["player_elo_p1"],
                    m["archetype_p2"], m["archetype_p1"],
                    m["color_p2"], m["deck_id_p2"], m["cards_p2"])

        # P2 losses == P1 wins
        for _ in range(int(m["player1Wins"])):
            add_row(0, m["player2"], m["player1"],
                    m["player_elo_p2"], m["player_elo_p1"],
                    m["archetype_p2"], m["archetype_p1"],
                    m["color_p2"], m["deck_id_p2"], m["cards_p2"])

    df = pd.DataFrame(rows)
    print(f"Created {len(df)} game-level rows.")
    return df


# -------------------------------------------------------------------
# Main driver
# -------------------------------------------------------------------

def main():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    processed_dir = os.path.join(project_root, "data", "processed")

    print("=" * 80)
    print("Python Data Preparation for R GLMER Analysis")
    print("=" * 80)

    matches_df = prepare_data_for_r(processed_dir)
    matches_df, card_names = matches_df, None  # card_names unused now

    games_df = create_game_level_data_fast(matches_df)

    outpath = os.path.join(processed_dir, "games_for_r.csv")
    print(f"\nSaving to: {outpath}")
    games_df.to_csv(outpath, index=False)

    print("\nDone! Now run:")
    print(f"  Rscript glmer_analysis.R {outpath} {os.path.join(processed_dir, 'card_glmer_results_from_r.csv')} 5")
    print("=" * 80)


if __name__ == "__main__":
    main()

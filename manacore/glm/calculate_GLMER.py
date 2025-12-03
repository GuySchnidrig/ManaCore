import os
import sys
import pandas as pd
import numpy as np
import subprocess
import tempfile
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Add project root to sys.path
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if root_path not in sys.path:
    sys.path.insert(0, root_path)


def load_data(processed_dir: str):
    """Load and validate required data files."""
    if not os.path.exists(processed_dir):
        raise FileNotFoundError(f"Directory not found: {processed_dir}")
    
    matches_df = pd.read_csv(os.path.join(processed_dir, "matches.csv"))
    decks_df = pd.read_csv(os.path.join(processed_dir, "drafted_decks.csv"))
    standings_df = pd.read_csv(os.path.join(processed_dir, "standings.csv"))
    
    print(f"Loaded {len(matches_df)} matches, {len(decks_df)} deck-card pairs")
    return matches_df, decks_df, standings_df


def get_player_ratings(processed_dir: str, standings_df: pd.DataFrame):
    """Extract player ELO ratings."""
    elo_df = pd.read_csv(os.path.join(processed_dir, "elo_development.csv"))
    player_elo = elo_df.sort_values('draft_id').groupby('player').tail(1)[['player', 'elo']]
    player_elo.columns = ['player_name', 'player_elo']
    
    print(f"Ratings for {len(player_elo)} players (range: {player_elo['player_elo'].min():.0f}-{player_elo['player_elo'].max():.0f})")
    return player_elo


def prepare_data_for_r(processed_dir: str):
    """Prepare data and merge into game-level observations."""
    matches_df, decks_df, standings_df = load_data(processed_dir)
    player_elo = get_player_ratings(processed_dir, standings_df)
    
    # Get unique decks
    unique_decks = decks_df.groupby('deck_id').agg({
        'player': 'first', 'archetype': 'first', 
        'decktype': 'first', 'draft_id': 'first', 'season_id': 'first',
        'deck_color_short': 'first'
    }).reset_index()
    
    # Merge matches with deck info
    data = matches_df.merge(
        unique_decks, left_on=['draft_id', 'player1'], 
        right_on=['draft_id', 'player'], how='left'
    ).drop('player', axis=1).rename(columns={
        'deck_id': 'deck_id_p1', 'archetype': 'archetype_p1', 
        'deck_color_short': 'color_p1'
    })
    
    data = data.merge(
        unique_decks, left_on=['draft_id', 'player2'],
        right_on=['draft_id', 'player'], how='left'
    ).drop('player', axis=1).rename(columns={
        'deck_id': 'deck_id_p2', 'archetype': 'archetype_p2',
        'deck_color_short': 'color_p2'
    })
    
    # Add ELO ratings
    data = data.merge(player_elo, left_on='player1', right_on='player_name', how='left'
                     ).drop('player_name', axis=1).rename(columns={'player_elo': 'player_elo_p1'})
    data = data.merge(player_elo, left_on='player2', right_on='player_name', how='left'
                     ).drop('player_name', axis=1).rename(columns={'player_elo': 'player_elo_p2'})
    
    # Get deck cards
    deck_cards = decks_df.groupby('deck_id')['scryfallId'].apply(list).reset_index()
    deck_cards.columns = ['deck_id', 'cards']
    
    data = data.merge(deck_cards, left_on='deck_id_p1', right_on='deck_id', how='left'
                     ).drop('deck_id', axis=1).rename(columns={'cards': 'cards_p1'})
    data = data.merge(deck_cards, left_on='deck_id_p2', right_on='deck_id', how='left'
                     ).drop('deck_id', axis=1).rename(columns={'cards': 'cards_p2'})
    
    print(f"Prepared {len(data)} matches for analysis")
    
    # Get card names for later
    card_names = decks_df[['scryfallId', 'card_name']].drop_duplicates()
    
    return data, card_names


def create_game_level_data_for_r(matches_df, card_names):
    """
    Create game-level observations with all cards.
    Each row is one game with indicators for which cards were in the deck.
    """
    games = []
    valid_matches = matches_df.dropna(subset=['deck_id_p1', 'deck_id_p2', 
                                               'archetype_p1', 'archetype_p2', 
                                               'color_p1', 'color_p2'])
    
    print(f"\nCreating game-level data from {len(valid_matches)} matches...")
    
    # Get all unique cards
    all_cards = set()
    for _, match in valid_matches.iterrows():
        p1_cards = match['cards_p1'] if isinstance(match['cards_p1'], list) else []
        p2_cards = match['cards_p2'] if isinstance(match['cards_p2'], list) else []
        all_cards.update(p1_cards)
        all_cards.update(p2_cards)
    
    print(f"Found {len(all_cards)} unique cards across all decks")
    
    for idx, match in valid_matches.iterrows():
        if idx % 100 == 0:
            print(f"  Processing match {idx}/{len(valid_matches)}")
        
        p1_cards = set(match['cards_p1']) if isinstance(match['cards_p1'], list) else set()
        p2_cards = set(match['cards_p2']) if isinstance(match['cards_p2'], list) else set()
        
        # Helper to create game observation
        def add_games(n_games, win, player, opp, p_elo, o_elo, arch, arch_opp, 
                     deck_id, cards_in_deck, color):
            for _ in range(int(n_games)):
                # For each card, mark if it's in this deck
                for card_id in all_cards:
                    games.append({
                        'win': win,
                        'player_name': player,
                        'opponent_name': opp,
                        'player_elo': p_elo,
                        'opponent_elo': o_elo,
                        'elo_diff': p_elo - o_elo,
                        'elo_mean': (p_elo + o_elo) / 2,
                        'archetype': arch,
                        'archetype_opponent': arch_opp,
                        'color': color,
                        'deck_id': deck_id,
                        'draft_id': match['draft_id'],
                        'card_id': card_id,
                        'has_card': 1 if card_id in cards_in_deck else 0
                    })
        
        # P1 wins and losses
        add_games(match['player1Wins'], 1, match['player1'], match['player2'],
                 match['player_elo_p1'], match['player_elo_p2'],
                 match['archetype_p1'], match['archetype_p2'], 
                 match['deck_id_p1'], p1_cards, match['color_p1'])
        add_games(match['player2Wins'], 0, match['player1'], match['player2'],
                 match['player_elo_p1'], match['player_elo_p2'],
                 match['archetype_p1'], match['archetype_p2'], 
                 match['deck_id_p1'], p1_cards, match['color_p1'])
        
        # P2 wins and losses
        add_games(match['player2Wins'], 1, match['player2'], match['player1'],
                 match['player_elo_p2'], match['player_elo_p1'],
                 match['archetype_p2'], match['archetype_p1'], 
                 match['deck_id_p2'], p2_cards, match['color_p2'])
        add_games(match['player1Wins'], 0, match['player2'], match['player1'],
                 match['player_elo_p2'], match['player_elo_p1'],
                 match['archetype_p2'], match['archetype_p1'], 
                 match['deck_id_p2'], p2_cards, match['color_p2'])
    
    games_df = pd.DataFrame(games)
    
    # Add card names
    games_df = games_df.merge(card_names, left_on='card_id', right_on='scryfallId', how='left')
    games_df = games_df.drop('scryfallId', axis=1)
    
    print(f"Created {len(games_df)} game-card observations")
    print(f"Unique games: {games_df.groupby(['player_name', 'draft_id', 'opponent_name']).ngroups}")
    
    return games_df


def run_r_glmer_analysis(games_csv_path, output_csv_path, r_script_path, min_games=3):
    """Call R script to run GLMER analysis."""
    
    # Check if R is available
    try:
        result = subprocess.run(['Rscript', '--version'], 
                              capture_output=True, text=True, timeout=5)
        print(f"R version: {result.stdout.strip()}")
    except (subprocess.TimeoutExpired, FileNotFoundError):
        raise RuntimeError("Rscript not found. Please install R and ensure it's in your PATH.")
    
    # Check if R script exists
    if not os.path.exists(r_script_path):
        raise FileNotFoundError(f"R script not found: {r_script_path}")
    
    # Run R script
    print(f"\n{'='*80}")
    print("Running R GLMER Analysis")
    print(f"{'='*80}\n")
    
    cmd = ['Rscript', r_script_path, games_csv_path, output_csv_path, str(min_games)]
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=3600)
        print(result.stdout)
        if result.stderr:
            print("R Warnings/Messages:", result.stderr)
    except subprocess.CalledProcessError as e:
        print("R script failed!")
        print("STDOUT:", e.stdout)
        print("STDERR:", e.stderr)
        raise
    except subprocess.TimeoutExpired:
        raise RuntimeError("R script timed out after 1 hour")


def main():
    # Configuration
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    processed_dir = os.path.join(project_root, "data", "processed")
    r_script_path = os.path.join(os.path.dirname(__file__), "glmer_analysis.R")
    
    print(f"{'='*80}")
    print("Python Data Preparation for R GLMER Analysis")
    print(f"{'='*80}\n")
    
    # Prepare data
    matches_df, card_names = prepare_data_for_r(processed_dir)
    games_df = create_game_level_data_for_r(matches_df, card_names)
    
    # Save to temporary CSV for R
    temp_csv = os.path.join(processed_dir, "games_for_r.csv")
    print(f"\nSaving game-level data to: {temp_csv}")
    games_df.to_csv(temp_csv, index=False)
    
    # Output path for R results
    output_csv = os.path.join(processed_dir, "card_glmer_results_from_r.csv")
    
    # Run R analysis
    run_r_glmer_analysis(temp_csv, output_csv, r_script_path, min_games=5)
    
    # Load and display results
    print(f"\n{'='*80}")
    print("Loading Results from R")
    print(f"{'='*80}\n")
    
    results_df = pd.read_csv(output_csv)
    
    print(f"Loaded {len(results_df)} card results")
    print(f"Significant cards: {results_df['significant'].sum()}")
    
    print(f"\n{'='*80}")
    print("Top 10 Cards (from R GLMER)")
    print(f"{'='*80}")
    print(results_df.nlargest(10, 'coefficient')[
        ['card_name', 'coefficient', 'p_value', 'fdr_p', 'odds_ratio']
    ].to_string(index=False))
    
    print(f"\n{'='*80}")
    print("Bottom 10 Cards (from R GLMER)")
    print(f"{'='*80}")
    print(results_df.nsmallest(10, 'coefficient')[
        ['card_name', 'coefficient', 'p_value', 'fdr_p', 'odds_ratio']
    ].to_string(index=False))
    
    print(f"\n{'='*80}")
    print("Analysis Complete!")
    print(f"Results saved to: {output_csv}")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
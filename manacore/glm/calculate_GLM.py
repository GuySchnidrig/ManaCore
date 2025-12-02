import os
import sys
import pandas as pd
import numpy as np
from statsmodels.formula.api import glm
from statsmodels.genmod.families import Binomial
from statsmodels.stats.multitest import multipletests
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
    
    # Load datasets
    matches_df = pd.read_csv(os.path.join(processed_dir, "matches.csv"))
    decks_df = pd.read_csv(os.path.join(processed_dir, "drafted_decks.csv"))
    standings_df = pd.read_csv(os.path.join(processed_dir, "standings.csv"))
    
    print(f"Loaded {len(matches_df)} matches, {len(decks_df)} deck-card pairs")
    return matches_df, decks_df, standings_df

def get_player_ratings(processed_dir: str, standings_df: pd.DataFrame):
    """Extract player ELO ratings or calculate proxy."""
    elo_df = pd.read_csv(os.path.join(processed_dir, "elo_development.csv"))
    player_elo = elo_df.sort_values('draft_id').groupby('player').tail(1)[['player', 'elo']]
    player_elo.columns = ['player_name', 'player_elo']
    
    print(f"Ratings for {len(player_elo)} players (range: {player_elo['player_elo'].min():.0f}-{player_elo['player_elo'].max():.0f})")
    return player_elo

def prepare_glm_data(processed_dir: str):
    """Prepare merged dataset for GLM analysis."""
    matches_df, decks_df, standings_df = load_data(processed_dir)
    player_elo = get_player_ratings(processed_dir, standings_df)
    
    # Get unique decks
    unique_decks = decks_df.groupby('deck_id').agg({
        'player': 'first', 'archetype': 'first', 
        'decktype': 'first', 'draft_id': 'first', 'season_id': 'first'
    }).reset_index()
    
    # Merge matches with deck info for both players
    data = matches_df.merge(
        unique_decks, left_on=['draft_id', 'player1'], 
        right_on=['draft_id', 'player'], how='left'
    ).drop('player', axis=1).rename(columns={
        'deck_id': 'deck_id_p1', 'archetype': 'archetype_p1'
    })
    
    data = data.merge(
        unique_decks, left_on=['draft_id', 'player2'],
        right_on=['draft_id', 'player'], how='left'
    ).drop('player', axis=1).rename(columns={
        'deck_id': 'deck_id_p2', 'archetype': 'archetype_p2'
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
    return data, decks_df[['deck_id', 'scryfallId', 'card_name']]

def create_game_level_data(matches_df, card_of_interest=None):
    """Create game-level observations for GLM."""
    games = []
    valid_matches = matches_df.dropna(subset=['deck_id_p1', 'deck_id_p2', 'archetype_p1', 'archetype_p2'])
    
    for _, match in valid_matches.iterrows():
        p1_cards = match['cards_p1'] if isinstance(match['cards_p1'], list) else []
        p2_cards = match['cards_p2'] if isinstance(match['cards_p2'], list) else []
        
        # Helper to create game observation
        def add_games(n_games, win, player, opp, p_elo, o_elo, arch, arch_opp, deck_id, cards):
            for _ in range(int(n_games)):
                game = {
                    'win': win, 'player_elo': p_elo, 'opponent_elo': o_elo,
                    'archetype': arch, 'archetype_opponent': arch_opp,
                    'draft_id': match['draft_id'], 'player_name': player,
                    'opponent_name': opp, 'deck_id': deck_id
                }
                if card_of_interest:
                    game['has_card'] = 1 if card_of_interest in cards else 0
                games.append(game)
        
        # P1 wins and losses
        add_games(match['player1Wins'], 1, match['player1'], match['player2'],
                 match['player_elo_p1'], match['player_elo_p2'],
                 match['archetype_p1'], match['archetype_p2'], match['deck_id_p1'], p1_cards)
        add_games(match['player2Wins'], 0, match['player1'], match['player2'],
                 match['player_elo_p1'], match['player_elo_p2'],
                 match['archetype_p1'], match['archetype_p2'], match['deck_id_p1'], p1_cards)
        
        # P2 wins and losses
        add_games(match['player2Wins'], 1, match['player2'], match['player1'],
                 match['player_elo_p2'], match['player_elo_p1'],
                 match['archetype_p2'], match['archetype_p1'], match['deck_id_p2'], p2_cards)
        add_games(match['player1Wins'], 0, match['player2'], match['player1'],
                 match['player_elo_p2'], match['player_elo_p1'],
                 match['archetype_p2'], match['archetype_p1'], match['deck_id_p2'], p2_cards)
    
    games_df = pd.DataFrame(games)
    print(f"Created {len(games_df)} game observations")
    return games_df

def run_glm_analysis(matches_df, card_of_interest=None):
    """Run improved GLM analysis with:
       - elo_diff + elo_mean
       - FDR-corrected p-values for has_card
    """
    
    games_df = create_game_level_data(matches_df, card_of_interest)
    games_df = games_df.dropna(subset=['player_elo', 'opponent_elo',
                                       'archetype', 'archetype_opponent'])

    # Compute elo descriptors (non-collinear)
    games_df['elo_diff'] = games_df['player_elo'] - games_df['opponent_elo']
    games_df['elo_mean'] = (games_df['player_elo'] + games_df['opponent_elo']) / 2

    # Convert to categorical where appropriate
    for col in ['archetype', 'archetype_opponent']:
        games_df[col] = pd.Categorical(games_df[col])

    print(f"\nGames: {len(games_df)} | Win rate: {games_df['win'].mean():.3f}")
    if card_of_interest:
        print(f"With card: {games_df['has_card'].sum()} games "
              f"({games_df[games_df['has_card']==1]['win'].mean():.3f} win rate)")

    # Build formula without draft_id
    formula = 'win ~ elo_diff + elo_mean'
    if card_of_interest:
        formula += ' + has_card'
    formula += ' + C(archetype) + C(archetype_opponent)'

    print(f"\nFitting: {formula}")

    # Fit GLM (no clustering by draft)
    model = glm(formula, data=games_df, family=Binomial())
    result = model.fit()

    print("\n" + "="*80)
    print(result.summary())

    # Card-specific output with FDR correction
    if card_of_interest:
        coef = result.params['has_card']
        raw_p = result.pvalues['has_card']

        # FDR correction (still works even though this is 1 p-value)
        _, fdr_p, _, _ = multipletests([raw_p], method='fdr_bh')

        print(f"\n{'='*80}")
        print("CARD EFFECT (Corrected):")
        print(f"  Coefficient:        {coef:.4f}")
        print(f"  Raw p-value:        {raw_p:.4f}")
        print(f"  FDR-adjusted p:     {fdr_p[0]:.4f}")
        print(f"  Odds Ratio:         {np.exp(coef):.4f}")

    return result, games_df


def analyze_all_cards(matches_df, mainboard_df, min_decks=10):
    """Analyze all cards appearing in at least min_decks decks using the same GLM approach as run_glm_analysis."""
    
    # Count decks per card
    card_counts = mainboard_df.groupby('scryfallId')['deck_id'].nunique()
    cards_to_analyze = card_counts[card_counts >= min_decks].index.tolist()
    
    print(f"\nAnalyzing {len(cards_to_analyze)} cards (appearing in {min_decks}+ decks)")
    if len(cards_to_analyze) == 0:
        print("No cards meet the threshold. Try lowering min_decks.")
        return pd.DataFrame()
    
    results = []
    
    for i, card_id in enumerate(cards_to_analyze, 1):
        if i % 10 == 0 or i == 1:
            print(f"Progress: {i}/{len(cards_to_analyze)}")
        
        try:
            # Prepare game-level data for this card
            games_df = create_game_level_data(matches_df, card_of_interest=card_id)
            games_df = games_df.dropna(subset=['player_elo', 'opponent_elo', 'archetype', 'archetype_opponent'])
            
            # Skip if too few games with card
            if games_df['has_card'].sum() < 10:
                continue
            
            # Compute elo descriptors
            games_df['elo_diff'] = games_df['player_elo'] - games_df['opponent_elo']
            games_df['elo_mean'] = (games_df['player_elo'] + games_df['opponent_elo']) / 2
            
            # Convert archetypes to categorical
            for col in ['archetype', 'archetype_opponent']:
                games_df[col] = pd.Categorical(games_df[col])
            
            # Fit GLM
            formula = 'win ~ elo_diff + elo_mean + has_card + C(archetype) + C(archetype_opponent)'
            model = glm(formula, data=games_df, family=Binomial())
            result = model.fit()
            
            # Extract card effect
            card_name = mainboard_df[mainboard_df['scryfallId'] == card_id]['card_name'].iloc[0]
            coef = result.params['has_card']
            raw_p = result.pvalues['has_card']
            odds_ratio = np.exp(coef)
            
            # Append results
            results.append({
                'scryfallId': card_id,
                'card_name': card_name,
                'coefficient': coef,
                'p_value': raw_p,
                'odds_ratio': odds_ratio,
                'games_with_card': int(games_df['has_card'].sum()),
                'total_games': len(games_df),
                'win_rate_with': games_df[games_df['has_card']==1]['win'].mean(),
                'win_rate_without': games_df[games_df['has_card']==0]['win'].mean(),
                'decks_with_card': int(card_counts[card_id])
            })
        except Exception as e:
            print(f"  Error analyzing {card_id}: {str(e)[:50]}")
            continue
    
    # Compile results
    if len(results) == 0:
        print("No cards successfully analyzed.")
        return pd.DataFrame()
    
    results_df = pd.DataFrame(results)
    
    # FDR correction across all cards
    if not results_df.empty:
        _, fdr_pvals, _, _ = multipletests(results_df['p_value'], method='fdr_bh')
        results_df['fdr_p'] = fdr_pvals
        results_df['significant'] = results_df['fdr_p'] < 0.05
        results_df = results_df.sort_values('coefficient', ascending=False)
    
    return results_df

if __name__ == "__main__":
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    processed_dir = os.path.join(project_root, "data", "processed")
    
    # Prepare data
    matches_df, mainboard_df = prepare_glm_data(processed_dir)
    
    # Run general model
    print("\n" + "="*80 + "\nGENERAL MODEL\n" + "="*80)
    run_glm_analysis(matches_df)
    
    # Analyze all cards
    print(f"\n{'='*80}\nANALYZING ALL CARDS\n{'='*80}")
    results_df = analyze_all_cards(matches_df, mainboard_df, min_decks=5)
    
    if len(results_df) == 0:
        print("No results to save.")
    else:
        # Save results
        output_path = os.path.join(processed_dir, "card_glm_results.csv")
        results_df.to_csv(output_path, index=False)
        print(f"\nResults saved to: {output_path}")
        
        # Display top/bottom cards
        print(f"\n{'='*80}\nTOP 10 CARDS (Highest Win Impact)\n{'='*80}")
        display_cols = ['card_name', 'coefficient', 'p_value', 'odds_ratio', 'games_with_card', 'decks_with_card']
        print(results_df[display_cols].head(10).to_string(index=False))
        
        print(f"\n{'='*80}\nBOTTOM 10 CARDS (Lowest Win Impact)\n{'='*80}")
        print(results_df[display_cols].tail(10).to_string(index=False))
        
        print(f"\n{'='*80}\nSIGNIFICANT CARDS (p < 0.05): {results_df['significant'].sum()}/{len(results_df)}\n{'='*80}")
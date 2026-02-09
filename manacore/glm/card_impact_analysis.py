#!/usr/bin/env python3
"""
=======================================================================
CARD IMPACT ANALYSIS: Progressive Model Complexity
=======================================================================
This script compares models with increasing complexity:
Version 0: win ~ has_card + elo_diff (baseline)
Version 1: win ~ has_card + elo_diff + archetype
Version 2: win ~ has_card + archetype + archetype_opponent
=======================================================================
"""

import sys
import json
import warnings
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import statsmodels.api as sm
import statsmodels.formula.api as smf
from statsmodels.tools.sm_exceptions import ConvergenceWarning
from scipy import stats

# Suppress convergence warnings for cleaner output
warnings.filterwarnings('ignore', category=ConvergenceWarning)

# Try to import sklearn for ridge regression
SKLEARN_AVAILABLE = False
try:
    from sklearn.linear_model import RidgeCV
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
    print("scikit-learn loaded successfully - Ridge regularization will be available\n")
except ImportError:
    print("Note: scikit-learn not available - skipping ridge regularization")
    print("To enable: pip install scikit-learn\n")


def load_game_data(input_csv: str) -> pd.DataFrame:
    """Load and parse game-level data with JSON card lists."""
    games_df = pd.read_csv(input_csv)
    
    # Parse JSON card lists
    def parse_cards(x):
        if pd.isna(x) or str(x).strip() == "":
            return []
        try:
            return json.loads(x)
        except:
            return []
    
    games_df['cards'] = games_df['cards'].apply(parse_cards)
    
    print(f"Loaded {len(games_df)} games")
    return games_df


def load_card_names(input_csv: str) -> Optional[pd.DataFrame]:
    """Load card name lookup table if available."""
    card_lookup_path = Path(input_csv).parent / "card_lookup.csv"
    
    if card_lookup_path.exists():
        card_names_df = pd.read_csv(card_lookup_path)
        print(f"Loaded card names for {len(card_names_df)} cards")
        return card_names_df
    else:
        print("Warning: card_lookup.csv not found, card names will not be available")
        return None


def filter_eligible_cards(games_df: pd.DataFrame, min_games: int) -> List[str]:
    """Filter cards that appear in at least min_games."""
    all_cards = [card for cards_list in games_df['cards'] for card in cards_list]
    card_counts = pd.Series(all_cards).value_counts()
    eligible_cards = card_counts[card_counts >= min_games].index.tolist()
    
    print(f"Analyzing {len(eligible_cards)} cards (minimum {min_games} games)\n")
    return eligible_cards


def create_card_matrix(games_df: pd.DataFrame, eligible_cards: List[str]) -> pd.DataFrame:
    """Create binary card-game matrix (vectorized)."""
    print("Precomputing card-game matrix (vectorized)...")
    
    card_matrix = pd.DataFrame(
        {card: games_df['cards'].apply(lambda x: card in x).astype(int) 
         for card in eligible_cards},
        index=games_df.index
    )
    
    print(f"Card matrix created: {card_matrix.shape[0]} games x {card_matrix.shape[1]} cards\n")
    return card_matrix


def fit_glm_model(formula: str, data: pd.DataFrame) -> Optional[sm.GLM]:
    """Fit a logistic regression model."""
    try:
        model = smf.glm(formula, data=data, family=sm.families.Binomial()).fit()
        return model
    except Exception as e:
        return None


def compute_win_rate_lift(
    model: sm.GLM, 
    df_model: pd.DataFrame, 
    formula: str,
    global_wr: float
) -> Tuple[float, float]:
    """
    Compute marginal win-rate lift (ΔP).
    Returns average treatment effect across observed distribution.
    """
    try:
        # Create two versions of the data: with and without the card
        df_with = df_model.copy()
        df_with['has_card'] = 1
        df_without = df_model.copy()
        df_without['has_card'] = 0
        
        # Predict probabilities
        p_with = model.predict(df_with)
        p_without = model.predict(df_without)
        
        # Average treatment effect (in probability units)
        win_rate_lift_prob = (p_with - p_without).mean()
        
        # Also compute at mean covariate values for reference
        mean_data = pd.DataFrame({
            'elo_diff': [df_model['elo_diff'].mean()],
            'has_card': [0]
        })
        
        # Add archetype if it exists in the model
        if 'archetype' in formula:
            most_common_archetype = df_model['archetype'].mode()[0]
            mean_data['archetype'] = most_common_archetype
        
        # Add archetype_opponent if it exists in the model
        if 'archetype_opponent' in formula:
            most_common_opp = df_model['archetype_opponent'].mode()[0]
            mean_data['archetype_opponent'] = most_common_opp
        
        mean_data_with = mean_data.copy()
        mean_data_with['has_card'] = 1
        
        p_with_mean = model.predict(mean_data_with)[0]
        p_without_mean = model.predict(mean_data)[0]
        win_rate_lift_mean_prob = p_with_mean - p_without_mean
        
        return win_rate_lift_prob, win_rate_lift_mean_prob
        
    except Exception as e:
        return np.nan, np.nan


def analyze_card_with_models(
    card: str,
    card_idx: int,
    total_cards: int,
    card_matrix: pd.DataFrame,
    games_df: pd.DataFrame,
    model_versions: Dict,
    global_wr: float
) -> Dict[str, pd.DataFrame]:
    """Analyze a single card across all model versions."""
    
    if (card_idx + 1) % 25 == 0:
        print(f"  Progress: {card_idx + 1}/{total_cards}")
    
    # Use precomputed matrix
    games_df = games_df.copy()
    games_df['has_card'] = card_matrix[card]
    
    # Prepare data with factors
    df_model = games_df.copy()
    df_model['archetype'] = df_model['archetype'].astype('category')
    df_model['archetype_opponent'] = df_model['archetype_opponent'].astype('category')
    
    version_results = {}
    
    for version_id, version_info in model_versions.items():
        model = fit_glm_model(version_info['formula'], df_model)
        
        if model is not None and 'has_card' in model.params.index:
            params = model.params
            bse = model.bse
            pvalues = model.pvalues
            
            est = params['has_card']
            se = bse['has_card']
            z = est / se
            p = pvalues['has_card']
            
            # Check for separation (infinite or very large coefficients)
            separation_warning = abs(est) > 5 or se > 3
            if separation_warning and card_idx == 0:
                print(f"  WARNING: Possible separation for card {card} (coef = {est:.2f}, SE = {se:.2f})")
            
            # Get AIC for model comparison
            model_aic = model.aic
            
            # Compute marginal win-rate lift
            win_rate_lift_prob, win_rate_lift_mean_prob = compute_win_rate_lift(
                model, df_model, version_info['formula'], global_wr
            )
            
            # Debug output for first card
            if card_idx == 0:
                print(f"  Debug for card {card} ({version_id}):")
                print(f"    win_rate_lift_prob: {win_rate_lift_prob:.6f}")
                print(f"    win_rate_lift_mean_prob: {win_rate_lift_mean_prob:.6f}")
            
            version_results[version_id] = pd.DataFrame([{
                'card_id': card,
                'coef': est,
                'se': se,
                'z': z,
                'p': p,
                'or': np.exp(est),
                'or_lower': np.exp(est - 1.96 * se),
                'or_upper': np.exp(est + 1.96 * se),
                'aic': model_aic,
                'separation': separation_warning,
                'win_rate_lift_prob': win_rate_lift_prob,
                'win_rate_lift_mean_prob': win_rate_lift_mean_prob
            }])
        else:
            version_results[version_id] = pd.DataFrame()
    
    return version_results


def fit_ridge_regression(
    card_matrix: pd.DataFrame,
    games_df: pd.DataFrame,
    eligible_cards: List[str]
) -> Optional[pd.DataFrame]:
    """Fit ridge regression model for all cards simultaneously."""
    
    if not SKLEARN_AVAILABLE:
        return None
    
    print("\n=== Ridge Regression (L2 Regularization) ===")
    print("Fitting ridge models for all cards simultaneously...")
    
    try:
        # Prepare design matrix
        X = card_matrix.values
        y = games_df['win'].values
        
        # Add control variables (elo_diff)
        X_controls = np.column_stack([X, games_df['elo_diff'].values])
        
        # Standardize features for better regularization
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X_controls)
        
        # Fit ridge model with cross-validation to select alpha
        print("Running cross-validation to select alpha...")
        alphas = np.logspace(-3, 3, 50)
        ridge_cv = RidgeCV(
        alphas=alphas,
        cv=5,
        scoring='neg_mean_squared_error')
        ridge_cv.fit(X_scaled, y)
        
        # Extract coefficients for cards (exclude elo_diff)
        ridge_coefs = ridge_cv.coef_[:len(eligible_cards)]
        
        ridge_results = pd.DataFrame({
            'card_id': eligible_cards,
            'ridge_coef': ridge_coefs,
            'ridge_or': np.exp(ridge_coefs),
            'ridge_alpha_optimal': ridge_cv.alpha_
        })
        
        print(f"Ridge regression completed for {len(ridge_results)} cards")
        print(f"Optimal alpha: {ridge_cv.alpha_:.6f}")
        
        return ridge_results
        
    except Exception as e:
        print(f"Ridge regression failed: {e}")
        return None


def compute_archetype_strength(
    input_csv: str,
    output_csv: str
) -> Optional[pd.DataFrame]:
    """Compute empirical archetype strength from game win rates."""
    
    print("\n=== Computing Empirical Archetype Strength ===")
    print("Using archetype-level game win rates")
    
    archetype_wr_csv = Path(input_csv).parent / "archetype_game_winrate.csv"
    
    if not archetype_wr_csv.exists():
        print("No archetype_game_winrate.csv found — skipping analysis")
        return None
    
    print("Loading archetype game win rate data...")
    archetype_wr = pd.read_csv(archetype_wr_csv)
    
    # Focus on aggregate season if present
    if "Season-All" in archetype_wr['season_id'].values:
        archetype_wr = archetype_wr[archetype_wr['season_id'] == "Season-All"]
        print("Using Season-All aggregate data")
    else:
        print("WARNING: Season-All not found — using all rows")
    
    # Compute global win rate (weighted)
    global_wr = archetype_wr['games_won'].sum() / archetype_wr['games_played'].sum()
    print(f"Global game win rate: {global_wr:.4f}")
    
    # Compute archetype strength metrics
    archetype_strength = archetype_wr.copy()
    archetype_strength['archetype_win_rate_lift'] = (
        archetype_strength['game_win_rate'] - global_wr
    )
    archetype_strength['archetype_win_rate_lift_pct'] = (
        archetype_strength['archetype_win_rate_lift'] * 100
    )
    archetype_strength['archetype_win_rate_lift_rel'] = (
        archetype_strength['game_win_rate'] / global_wr
    )
    
    archetype_strength = archetype_strength.sort_values(
        'archetype_win_rate_lift', ascending=False
    )
    
    # Save results
    output_path = str(output_csv).replace('.csv', '_archetype_strength_empirical.csv')
    archetype_strength.to_csv(output_path, index=False)
    
    print("Empirical archetype strength analysis saved:")
    print(output_path)
    
    return archetype_strength


def main():
    """Main analysis pipeline."""
    
    # Parse command line arguments
    if len(sys.argv) < 3:
        print("Usage: python card_impact_analysis.py input.csv output.csv [min_games]")
        sys.exit(1)
    
    input_csv = Path(sys.argv[1])
    output_csv = Path(sys.argv[2])

    # Force output into same directory as input
    output_csv = input_csv.parent / output_csv.name
    min_games = int(sys.argv[3]) if len(sys.argv) >= 4 else 100
    
    # Input validation
    if not Path(input_csv).exists():
        print(f"Input file not found: {input_csv}")
        sys.exit(1)
    
    if min_games <= 0:
        print("min_games must be positive")
        sys.exit(1)
    
    print("=== Progressive Model Complexity Card Analysis ===\n")
    
    # Load data
    games_df = load_game_data(input_csv)
    card_names_df = load_card_names(input_csv)
    
    # Global win rate
    global_wr = games_df['win'].mean()
    print(f"Global win rate: {global_wr:.3f}\n")
    
    # Filter cards
    eligible_cards = filter_eligible_cards(games_df, min_games)
    
    # Create card matrix
    card_matrix = create_card_matrix(games_df, eligible_cards)
    
    # Define model versions
    model_versions = {
        'v0': {
            'name': 'Version 0: Baseline',
            'formula': 'win ~ has_card + elo_diff'
        },
        'v1': {
            'name': 'Version 1: + Archetype',
            'formula': 'win ~ has_card + elo_diff + archetype'
        },
        'v2': {
            'name': 'Version 2: Matchup Only',
            'formula': 'win ~ has_card + archetype + archetype_opponent'
        }
    }
    
    print("Fitting progressive model versions...\n")
    
    # Analyze all cards across all versions
    all_results = {version_id: [] for version_id in model_versions.keys()}
    
    for version_id, version_info in model_versions.items():
        print(f"=== {version_info['name']} ===")
        print(f"Formula: {version_info['formula']}\n")
        
        for i, card in enumerate(eligible_cards):
            version_results = analyze_card_with_models(
                card, i, len(eligible_cards),
                card_matrix, games_df,
                {version_id: version_info},
                global_wr
            )
            
            if version_id in version_results and not version_results[version_id].empty:
                all_results[version_id].append(version_results[version_id])
        
        # Combine results for this version
        if all_results[version_id]:
            all_results[version_id] = pd.concat(all_results[version_id], ignore_index=True)
            
            # Add version prefix to column names
            cols_to_rename = [col for col in all_results[version_id].columns if col != 'card_id']
            rename_dict = {col: f"{version_id}_{col}" for col in cols_to_rename}
            all_results[version_id] = all_results[version_id].rename(columns=rename_dict)
            
            print(f"Completed: {len(all_results[version_id])} cards with estimates")
            
            sep_col = f"{version_id}_separation"
            n_separation = all_results[version_id][sep_col].sum()
            if n_separation > 0:
                print(f"WARNING: {n_separation} cards with possible separation issues")
        else:
            all_results[version_id] = pd.DataFrame()
        
        print()
    
    # Combine all version results
    print("Combining results from all model versions...")
    final_results = all_results['v0']
    for version_id in list(model_versions.keys())[1:]:
        if not all_results[version_id].empty:
            final_results = final_results.merge(
                all_results[version_id],
                on='card_id',
                how='outer'
            )
    
    # Add game counts
    card_counts = pd.Series(
        [card for cards_list in games_df['cards'] for card in cards_list]
    ).value_counts()
    
    game_counts_df = pd.DataFrame({
        'card_id': eligible_cards,
        'n_games': [card_counts[card] for card in eligible_cards]
    })
    
    final_results = final_results.merge(game_counts_df, on='card_id', how='left')
    
    # FDR Correction
    print("\nApplying FDR correction and computing raw significance...")
    
    for version_id in model_versions.keys():
        p_col = f"{version_id}_p"
        p_adj_col = f"{version_id}_p_adj"
        sig_raw_col = f"{version_id}_significant_raw"
        sig_fdr_col = f"{version_id}_significant_fdr"
        
        if p_col in final_results.columns:
            # FDR correction using Benjamini-Hochberg
            from statsmodels.stats.multitest import multipletests
            _, p_adj, _, _ = multipletests(
                final_results[p_col].fillna(1),
                alpha=0.05,
                method='fdr_bh'
            )
            
            final_results[p_adj_col] = p_adj
            final_results[sig_raw_col] = final_results[p_col] < 0.05
            final_results[sig_fdr_col] = final_results[p_adj_col] < 0.05
    
    # Model Comparison Metrics & Effect Sizes
    print("Computing model comparison metrics and effect sizes...")
    
    final_results['best_aic'] = final_results[['v0_aic', 'v1_aic', 'v2_aic']].min(axis=1)
    final_results['aic_improvement_v0_to_v1'] = final_results['v0_aic'] - final_results['v1_aic']
    final_results['aic_improvement_v1_to_v2'] = final_results['v1_aic'] - final_results['v2_aic']
    
    # Effect size stability
    final_results['or_range'] = (
        final_results[['v0_or', 'v1_or', 'v2_or']].max(axis=1) -
        final_results[['v0_or', 'v1_or', 'v2_or']].min(axis=1)
    )
    final_results['or_mean'] = final_results[['v0_or', 'v1_or', 'v2_or']].mean(axis=1)
    final_results['or_cv'] = final_results['or_range'] / final_results['or_mean']
    
    # Confidence based on consistency and sample size
    def assign_confidence(row):
        if row['n_games'] >= 200 and row['or_cv'] < 0.1:
            return 'high'
        elif row['n_games'] >= 100 and row['or_cv'] < 0.2:
            return 'medium'
        else:
            return 'low'
    
    final_results['confidence'] = final_results.apply(assign_confidence, axis=1)
    
    # Add percentage and standardized versions
    for version_id in model_versions.keys():
        prob_col = f"{version_id}_win_rate_lift_prob"
        mean_prob_col = f"{version_id}_win_rate_lift_mean_prob"
        
        if prob_col in final_results.columns:
            pct_col = f"{version_id}_win_rate_lift_pct"
            mean_pct_col = f"{version_id}_win_rate_lift_mean_pct"
            std_col = f"{version_id}_win_rate_lift_std"
            mean_std_col = f"{version_id}_win_rate_lift_mean_std"
            
            final_results[pct_col] = final_results[prob_col] * 100
            final_results[mean_pct_col] = final_results[mean_prob_col] * 100
            final_results[std_col] = final_results[prob_col] / global_wr
            final_results[mean_std_col] = final_results[mean_prob_col] / global_wr
    
    # Sort by V2 OR
    final_results = final_results.sort_values('v2_or', ascending=False)
    
    # Save intermediate results (GLM only)
    intermediate_csv = output_csv.with_name(
        output_csv.stem + "_glm_only.csv"
    )
    
    if card_names_df is not None:
        final_results_checkpoint = final_results.merge(
            card_names_df,
            on='card_id',
            how='left'
        )
        # Reorder columns to put card_name second
        cols = ['card_id', 'card_name'] + [c for c in final_results_checkpoint.columns 
                                           if c not in ['card_id', 'card_name']]
        final_results_checkpoint = final_results_checkpoint[cols]
    else:
        final_results_checkpoint = final_results
    
    final_results_checkpoint.to_csv(intermediate_csv, index=False)
    print(f"\nCheckpoint saved: {intermediate_csv}")
    
    # Ridge Regression
    ridge_results = fit_ridge_regression(card_matrix, games_df, eligible_cards)
    
    if ridge_results is not None:
        final_results = final_results.merge(ridge_results, on='card_id', how='left')
    
    # Archetype Strength Analysis
    archetype_strength = compute_archetype_strength(input_csv, output_csv)
    
    # Add card names to final results
    if card_names_df is not None and 'card_name' not in final_results.columns:
        final_results = final_results.merge(card_names_df, on='card_id', how='left')
        # Reorder columns
        cols = ['card_id', 'card_name'] + [c for c in final_results.columns 
                                           if c not in ['card_id', 'card_name']]
        final_results = final_results[cols]
    
    # Save final results
    final_results.to_csv(output_csv, index=False)
    
    # Summary Statistics
    print("\n=== Analysis Complete ===")
    print(f"Total cards analyzed: {len(final_results)}")
    
    for version_id, version_info in model_versions.items():
        or_col = f"{version_id}_or"
        sig_raw_col = f"{version_id}_significant_raw"
        sig_fdr_col = f"{version_id}_significant_fdr"
        sep_col = f"{version_id}_separation"
        
        print(f"\n{version_info['name']}:")
        print(f"  Estimates: {final_results[or_col].notna().sum()}")
        print(f"  Significant (raw p < 0.05): {final_results[sig_raw_col].sum()}")
        print(f"  Significant (FDR < 0.05): {final_results[sig_fdr_col].sum()}")
        
        if sep_col in final_results.columns:
            n_sep = final_results[sep_col].sum()
            if n_sep > 0:
                print(f"  Separation warnings: {n_sep}")
    
    # Top Cards by Win-Rate Lift (V2)
    print("\n=== Top 10 Cards by Win-Rate Lift (V2 Matchup Model) ===")
    cols_v2_lift = [
        'card_id', 'card_name', 'v2_win_rate_lift_std', 'v2_win_rate_lift_pct',
        'v2_or', 'v2_p', 'v2_significant_raw', 'n_games', 'confidence'
    ]
    cols_v2_lift = [c for c in cols_v2_lift if c in final_results.columns]
    
    if 'v2_win_rate_lift_std' in final_results.columns:
        top_cards = final_results.dropna(subset=['v2_win_rate_lift_std']).nlargest(
            10, 'v2_win_rate_lift_std'
        )[cols_v2_lift]
        print(top_cards.to_string(index=False))
    
    # Model Progression Comparison
    print("\n=== Model Progression Comparison (Top 10 by games) ===")
    comparison_cols = [
        'card_id', 'card_name', 'n_games',
        'v0_or', 'v1_or', 'v2_or',
        'v2_win_rate_lift_pct', 'or_cv'
    ]
    comparison_cols = [c for c in comparison_cols if c in final_results.columns]
    
    progression = final_results.nlargest(10, 'n_games')[comparison_cols]
    print(progression.to_string(index=False))
    
    # Ridge vs V2 Comparison
    if ridge_results is not None:
        print("\n=== Ridge vs V2 Comparison (Top 10 by games) ===")
        comparison = final_results.dropna(subset=['ridge_or', 'v2_or']).copy()
        comparison['or_diff'] = (comparison['v2_or'] - comparison['ridge_or']).abs()
        
        ridge_comp_cols = ['card_id', 'card_name', 'n_games', 'v2_or', 'ridge_or', 'or_diff']
        ridge_comp_cols = [c for c in ridge_comp_cols if c in comparison.columns]
        
        ridge_comp = comparison.nlargest(10, 'n_games')[ridge_comp_cols]
        print(ridge_comp.to_string(index=False))
    
    print(f"\nResults saved to: {output_csv}")
    print(f"Intermediate results (GLM only) saved to: {intermediate_csv}")
    print("\nNote: Default minimum games threshold is 100.")
    print("Usage:")
    print("  python card_impact_analysis.py input.csv output.csv [min_games]")


if __name__ == "__main__":
    main()
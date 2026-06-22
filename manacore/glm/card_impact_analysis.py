#!/usr/bin/env python3
"""
=======================================================================
CARD IMPACT ANALYSIS: Progressive Model Complexity
=======================================================================
Version 0: win ~ has_card + elo_diff (baseline)
Version 1: win ~ has_card + elo_diff + archetype
Version 2: win ~ has_card + elo_diff + archetype + archetype_opponent

Group analysis: runs the same v0/v1/v2 models per card group
(Fetchlands, Shocklands, Fast Mana, etc.) using has_any_<group>
as the treatment variable. Saves to _group_analysis.csv.
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

warnings.filterwarnings('ignore', category=ConvergenceWarning)

SKLEARN_AVAILABLE = False
try:
    from sklearn.linear_model import RidgeCV
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
    print("scikit-learn loaded successfully - Ridge regularization will be available\n")
except ImportError:
    print("Note: scikit-learn not available - skipping ridge regularization")
    print("To enable: pip install scikit-learn\n")


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_game_data(input_csv: str) -> pd.DataFrame:
    games_df = pd.read_csv(input_csv)

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
    card_lookup_path = Path(input_csv).parent / "card_lookup.csv"
    if card_lookup_path.exists():
        card_names_df = pd.read_csv(card_lookup_path)
        print(f"Loaded card names for {len(card_names_df)} cards")
        return card_names_df
    else:
        print("Warning: card_lookup.csv not found, card names will not be available")
        return None


def load_card_groups(input_csv: str) -> Optional[Dict[str, dict]]:
    """
    Load card group definitions.
    Looks for card_groups.csv or vintage_cube_grouped.csv in the same dir.
    Expected columns: Category, Card Name
    Returns dict: slug -> {'display': str, 'cards': [lowercase names]}
    Cards can belong to multiple groups (multi-hot).
    """
    base_dir = Path(input_csv).parent
    script_dir = Path(__file__).resolve().parent  # Manacore/manacore/glm
    candidates = [
        script_dir.parent / "config" / "vintage_cube_grouped.csv",  # Manacore/manacore/config
        base_dir / "card_groups.csv",
        base_dir / "vintage_cube_grouped.csv",
    ]

    groups_path = None
    for p in candidates:
        if p.exists():
            groups_path = p
            break

    if groups_path is None:
        print("Note: No card_groups.csv or vintage_cube_grouped.csv found - skipping group analysis")
        return None

    df = pd.read_csv(groups_path)
    df.columns = [c.strip() for c in df.columns]

    cat_col = next((c for c in df.columns if c.lower() in ('category', 'group', 'group_name')), None)
    name_col = next((c for c in df.columns if c.lower() in ('card name', 'card_name', 'name', 'cardname')), None)

    if cat_col is None or name_col is None:
        print(f"Warning: card_groups file must have Category and Card Name columns (found: {list(df.columns)})")
        return None

    groups: Dict[str, dict] = {}
    duplicates_skipped = 0
    for _, row in df.iterrows():
        cat = str(row[cat_col]).strip()
        card = str(row[name_col]).strip().lower()
        slug = (cat.lower()
                .replace(" ", "_")
                .replace("+", "plus")
                .replace("-", "_"))
        if slug not in groups:
            groups[slug] = {'display': cat, 'cards': []}
        if card not in groups[slug]['cards']:
            groups[slug]['cards'].append(card)
        else:
            duplicates_skipped += 1

    print(f"Loaded {len(groups)} card groups from {groups_path.name}:")
    for slug, info in groups.items():
        print(f"  {info['display']}: {len(info['cards'])} cards")
    if duplicates_skipped:
        print(f"  ({duplicates_skipped} duplicate entries skipped)")
    print()
    return groups


# ---------------------------------------------------------------------------
# Card filtering & matrices
# ---------------------------------------------------------------------------

def filter_eligible_cards(games_df: pd.DataFrame, min_games: int) -> List[str]:
    all_cards = [card for cards_list in games_df['cards'] for card in cards_list]
    card_counts = pd.Series(all_cards).value_counts()
    eligible_cards = card_counts[card_counts >= min_games].index.tolist()
    print(f"Analyzing {len(eligible_cards)} cards (minimum {min_games} games)\n")
    return eligible_cards


def create_card_matrix(games_df: pd.DataFrame, eligible_cards: List[str]) -> pd.DataFrame:
    print("Precomputing card-game matrix (vectorized)...")
    card_matrix = pd.DataFrame(
        {card: games_df['cards'].apply(lambda x: card in x).astype(int)
         for card in eligible_cards},
        index=games_df.index
    )
    print(f"Card matrix created: {card_matrix.shape[0]} games x {card_matrix.shape[1]} cards\n")
    return card_matrix


def create_group_matrix(
    games_df: pd.DataFrame,
    card_groups: Dict[str, dict],
    card_names_df: Optional[pd.DataFrame]
) -> pd.DataFrame:
    """
    Derive has_any_<slug> binary column per game for each group.
    Cards are matched by name (case-insensitive), with optional card_id resolution.
    Multi-hot: a game can be positive for multiple groups.
    """
    print("Building group membership matrix...")

    # Build card_name -> card_id mapping if lookup table available
    name_to_id: Dict[str, str] = {}
    if card_names_df is not None and 'card_id' in card_names_df.columns and 'card_name' in card_names_df.columns:
        for _, row in card_names_df.iterrows():
            name_to_id[str(row['card_name']).strip().lower()] = str(row['card_id'])

    group_matrix = pd.DataFrame(index=games_df.index)

    for slug, info in card_groups.items():
        card_names_lower = info['cards']

        if name_to_id:
            target_ids = set()
            for cn in card_names_lower:
                cid = name_to_id.get(cn)
                if cid:
                    target_ids.add(cid)
            unresolved = [cn for cn in card_names_lower if cn not in name_to_id]
            target_ids.update(unresolved)
        else:
            target_ids = set(card_names_lower)

        col = f"has_any_{slug}"
        group_matrix[col] = games_df['cards'].apply(
            lambda deck: int(any(str(c).lower() in target_ids or c in target_ids for c in deck))
        )

        n_games = group_matrix[col].sum()
        print(f"  {info['display']}: {n_games} games with >=1 group member")

    print()
    return group_matrix


# ---------------------------------------------------------------------------
# Shared model helpers
# ---------------------------------------------------------------------------

def fit_glm_model(formula: str, data: pd.DataFrame):
    try:
        return smf.glm(formula, data=data, family=sm.families.Binomial()).fit()
    except Exception:
        return None


def compute_win_rate_lift(
    model,
    df_model: pd.DataFrame,
    formula: str,
    treatment_col: str,
    global_wr: float
) -> Tuple[float, float]:
    try:
        df_with = df_model.copy()
        df_with[treatment_col] = 1
        df_without = df_model.copy()
        df_without[treatment_col] = 0

        p_with = model.predict(df_with)
        p_without = model.predict(df_without)
        win_rate_lift_prob = (p_with - p_without).mean()

        mean_data = pd.DataFrame({'elo_diff': [df_model['elo_diff'].mean()], treatment_col: [0]})
        if 'archetype' in formula:
            mean_data['archetype'] = df_model['archetype'].mode()[0]
        if 'archetype_opponent' in formula:
            mean_data['archetype_opponent'] = df_model['archetype_opponent'].mode()[0]

        mean_data_with = mean_data.copy()
        mean_data_with[treatment_col] = 1

        p_with_mean = model.predict(mean_data_with)[0]
        p_without_mean = model.predict(mean_data)[0]
        win_rate_lift_mean_prob = p_with_mean - p_without_mean

        return win_rate_lift_prob, win_rate_lift_mean_prob
    except Exception:
        return np.nan, np.nan


# ---------------------------------------------------------------------------
# Per-card analysis
# ---------------------------------------------------------------------------

def analyze_card_with_models(
    card, card_idx, total_cards,
    card_matrix, games_df, model_versions, global_wr
) -> Dict[str, pd.DataFrame]:

    if (card_idx + 1) % 25 == 0:
        print(f"  Progress: {card_idx + 1}/{total_cards}")

    games_df = games_df.copy()
    games_df['has_card'] = card_matrix[card]

    df_model = games_df.copy()
    df_model['archetype'] = df_model['archetype'].astype('category')
    df_model['archetype_opponent'] = df_model['archetype_opponent'].astype('category')

    version_results = {}

    for version_id, version_info in model_versions.items():
        model = fit_glm_model(version_info['formula'], df_model)

        if model is not None and 'has_card' in model.params.index:
            est = model.params['has_card']
            se = model.bse['has_card']
            z = est / se
            p = model.pvalues['has_card']
            separation_warning = abs(est) > 5 or se > 3

            win_rate_lift_prob, win_rate_lift_mean_prob = compute_win_rate_lift(
                model, df_model, version_info['formula'], 'has_card', global_wr
            )

            if card_idx == 0:
                print(f"  Debug for card {card} ({version_id}):")
                print(f"    win_rate_lift_prob: {win_rate_lift_prob:.6f}")
                print(f"    win_rate_lift_mean_prob: {win_rate_lift_mean_prob:.6f}")

            version_results[version_id] = pd.DataFrame([{
                'card_id': card,
                'coef': est, 'se': se, 'z': z, 'p': p,
                'or': np.exp(est),
                'or_lower': np.exp(est - 1.96 * se),
                'or_upper': np.exp(est + 1.96 * se),
                'aic': model.aic,
                'separation': separation_warning,
                'win_rate_lift_prob': win_rate_lift_prob,
                'win_rate_lift_mean_prob': win_rate_lift_mean_prob
            }])
        else:
            version_results[version_id] = pd.DataFrame()

    return version_results


# ---------------------------------------------------------------------------
# Group analysis
# ---------------------------------------------------------------------------

def analyze_group_with_models(
    group_slug, group_display, group_idx, total_groups,
    group_col, group_matrix, games_df, model_versions, global_wr
) -> Dict[str, pd.DataFrame]:
    """Run v0/v1/v2 models for a single card group using has_any_<slug> as treatment."""

    print(f"  [{group_idx + 1}/{total_groups}] {group_display}")

    df_model = games_df.copy()
    df_model[group_col] = group_matrix[group_col]
    df_model['archetype'] = df_model['archetype'].astype('category')
    df_model['archetype_opponent'] = df_model['archetype_opponent'].astype('category')

    n_with = int(df_model[group_col].sum())
    n_without = len(df_model) - n_with

    if n_with < 10 or n_without < 10:
        print(f"    Skipping - insufficient variation (with={n_with}, without={n_without})")
        return {}

    version_results = {}

    for version_id, version_info in model_versions.items():
        # Replace the generic 'has_card' token with the actual group column name
        formula = version_info['formula'].replace('has_card', group_col)
        model = fit_glm_model(formula, df_model)

        if model is not None and group_col in model.params.index:
            est = model.params[group_col]
            se = model.bse[group_col]
            z = est / se
            p = model.pvalues[group_col]
            separation_warning = abs(est) > 5 or se > 3

            win_rate_lift_prob, win_rate_lift_mean_prob = compute_win_rate_lift(
                model, df_model, formula, group_col, global_wr
            )

            version_results[version_id] = pd.DataFrame([{
                'group_id': group_slug,
                'group_name': group_display,
                'n_games_with_group': n_with,
                'coef': est, 'se': se, 'z': z, 'p': p,
                'or': np.exp(est),
                'or_lower': np.exp(est - 1.96 * se),
                'or_upper': np.exp(est + 1.96 * se),
                'aic': model.aic,
                'separation': separation_warning,
                'win_rate_lift_prob': win_rate_lift_prob,
                'win_rate_lift_mean_prob': win_rate_lift_mean_prob
            }])
        else:
            version_results[version_id] = pd.DataFrame()

    return version_results


def run_group_analysis(
    games_df, card_groups, group_matrix, model_versions, global_wr, output_csv
) -> pd.DataFrame:

    print("\n" + "=" * 60)
    print("GROUP ANALYSIS")
    print("=" * 60)

    from statsmodels.stats.multitest import multipletests

    group_slugs = list(card_groups.keys())
    total_groups = len(group_slugs)
    all_group_results = {v: [] for v in model_versions.keys()}

    for version_id, version_info in model_versions.items():
        print(f"\n=== {version_info['name']} ===")

        for g_idx, slug in enumerate(group_slugs):
            group_col = f"has_any_{slug}"
            display = card_groups[slug]['display']

            version_results = analyze_group_with_models(
                slug, display, g_idx, total_groups,
                group_col, group_matrix, games_df,
                {version_id: version_info}, global_wr
            )

            if version_id in version_results and not version_results[version_id].empty:
                all_group_results[version_id].append(version_results[version_id])

        if all_group_results[version_id]:
            all_group_results[version_id] = pd.concat(
                all_group_results[version_id], ignore_index=True
            )
            id_cols = ('group_id', 'group_name', 'n_games_with_group')
            rename_dict = {
                c: f"{version_id}_{c}"
                for c in all_group_results[version_id].columns
                if c not in id_cols
            }
            all_group_results[version_id] = all_group_results[version_id].rename(columns=rename_dict)
        else:
            all_group_results[version_id] = pd.DataFrame()

    # Merge versions
    print("\nMerging group analysis versions...")
    id_cols = ['group_id', 'group_name', 'n_games_with_group']
    group_final = all_group_results['v0']
    for version_id in list(model_versions.keys())[1:]:
        if not all_group_results[version_id].empty:
            group_final = group_final.merge(
                all_group_results[version_id], on=id_cols, how='outer'
            )

    # FDR correction
    for version_id in model_versions.keys():
        p_col = f"{version_id}_p"
        if p_col in group_final.columns:
            _, p_adj, _, _ = multipletests(
                group_final[p_col].fillna(1), alpha=0.05, method='fdr_bh'
            )
            group_final[f"{version_id}_p_adj"] = p_adj
            group_final[f"{version_id}_significant_raw"] = group_final[p_col] < 0.05
            group_final[f"{version_id}_significant_fdr"] = p_adj < 0.05

    # Win-rate lift columns
    for version_id in model_versions.keys():
        prob_col = f"{version_id}_win_rate_lift_prob"
        mean_prob_col = f"{version_id}_win_rate_lift_mean_prob"
        if prob_col in group_final.columns:
            group_final[f"{version_id}_win_rate_lift_pct"] = group_final[prob_col] * 100
            group_final[f"{version_id}_win_rate_lift_mean_pct"] = group_final[mean_prob_col] * 100
            group_final[f"{version_id}_win_rate_lift_std"] = group_final[prob_col] / global_wr
            group_final[f"{version_id}_win_rate_lift_mean_std"] = group_final[mean_prob_col] / global_wr

    # OR stability
    or_cols = [f"{v}_or" for v in model_versions.keys() if f"{v}_or" in group_final.columns]
    if or_cols:
        group_final['or_range'] = group_final[or_cols].max(axis=1) - group_final[or_cols].min(axis=1)
        group_final['or_mean'] = group_final[or_cols].mean(axis=1)
        group_final['or_cv'] = group_final['or_range'] / group_final['or_mean']

    if 'v2_or' in group_final.columns:
        group_final = group_final.sort_values('v2_or', ascending=False)

    group_output = output_csv.with_name(output_csv.stem + "_group_analysis.csv")
    group_final.to_csv(group_output, index=False)
    print(f"\nGroup analysis saved: {group_output}")

    # Summary table
    print("\n=== Group Analysis Summary ===")
    summary_cols = ['group_name', 'n_games_with_group',
                    'v0_or', 'v1_or', 'v2_or',
                    'v2_win_rate_lift_pct', 'v2_p',
                    'v2_significant_raw', 'v2_significant_fdr']
    summary_cols = [c for c in summary_cols if c in group_final.columns]
    if summary_cols:
        print(group_final[summary_cols].to_string(index=False))

    return group_final


# ---------------------------------------------------------------------------
# Ridge regression
# ---------------------------------------------------------------------------

def fit_ridge_regression(card_matrix, games_df, eligible_cards):
    if not SKLEARN_AVAILABLE:
        return None

    print("\n=== Ridge Regression (L2 Regularization) ===")
    print("Fitting ridge models for all cards simultaneously...")

    try:
        X = card_matrix.values
        y = games_df['win'].values
        X_controls = np.column_stack([X, games_df['elo_diff'].values])
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X_controls)

        print("Running cross-validation to select alpha...")
        alphas = np.logspace(-3, 3, 50)
        ridge_cv = RidgeCV(alphas=alphas, cv=5, scoring='neg_mean_squared_error')
        ridge_cv.fit(X_scaled, y)

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


# ---------------------------------------------------------------------------
# Archetype strength
# ---------------------------------------------------------------------------

def compute_archetype_strength(input_csv, output_csv):
    print("\n=== Computing Empirical Archetype Strength ===")
    archetype_wr_csv = Path(input_csv).parent / "archetype_game_winrate.csv"

    if not archetype_wr_csv.exists():
        print("No archetype_game_winrate.csv found - skipping analysis")
        return None

    print("Loading archetype game win rate data...")
    archetype_wr = pd.read_csv(archetype_wr_csv)

    if "Season-All" in archetype_wr['season_id'].values:
        archetype_wr = archetype_wr[archetype_wr['season_id'] == "Season-All"]
        print("Using Season-All aggregate data")
    else:
        print("WARNING: Season-All not found - using all rows")

    global_wr = archetype_wr['games_won'].sum() / archetype_wr['games_played'].sum()
    print(f"Global game win rate: {global_wr:.4f}")

    archetype_strength = archetype_wr.copy()
    archetype_strength['archetype_win_rate_lift'] = archetype_strength['game_win_rate'] - global_wr
    archetype_strength['archetype_win_rate_lift_pct'] = archetype_strength['archetype_win_rate_lift'] * 100
    archetype_strength['archetype_win_rate_lift_rel'] = archetype_strength['game_win_rate'] / global_wr
    archetype_strength = archetype_strength.sort_values('archetype_win_rate_lift', ascending=False)

    output_path = str(output_csv).replace('.csv', '_archetype_strength_empirical.csv')
    archetype_strength.to_csv(output_path, index=False)
    print(f"Empirical archetype strength analysis saved:\n{output_path}")
    return archetype_strength


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 3:
        print("Usage: python card_impact_analysis.py input.csv output.csv [min_games]")
        sys.exit(1)

    input_csv = Path(sys.argv[1])
    output_csv = Path(sys.argv[2])
    output_csv = input_csv.parent / output_csv.name
    min_games = int(sys.argv[3]) if len(sys.argv) >= 4 else 100

    if not input_csv.exists():
        print(f"Input file not found: {input_csv}")
        sys.exit(1)
    if min_games <= 0:
        print("min_games must be positive")
        sys.exit(1)

    print("=== Progressive Model Complexity Card Analysis ===\n")

    games_df = load_game_data(input_csv)
    card_names_df = load_card_names(input_csv)
    card_groups = load_card_groups(input_csv)

    global_wr = games_df['win'].mean()
    print(f"Global win rate: {global_wr:.3f}\n")

    eligible_cards = filter_eligible_cards(games_df, min_games)
    card_matrix = create_card_matrix(games_df, eligible_cards)

    model_versions = {
        'v0': {'name': 'Version 0: Baseline',    'formula': 'win ~ has_card + elo_diff'},
        'v1': {'name': 'Version 1: + Archetype', 'formula': 'win ~ has_card + elo_diff + archetype'},
        'v2': {'name': 'Version 2: Matchup Only','formula': 'win ~ has_card + elo_diff + archetype + archetype_opponent'},
    }

    print("Fitting progressive model versions...\n")

    # --- Per-card analysis ---
    all_results = {v: [] for v in model_versions.keys()}

    for version_id, version_info in model_versions.items():
        print(f"=== {version_info['name']} ===")
        print(f"Formula: {version_info['formula']}\n")

        for i, card in enumerate(eligible_cards):
            version_results = analyze_card_with_models(
                card, i, len(eligible_cards),
                card_matrix, games_df,
                {version_id: version_info}, global_wr
            )
            if version_id in version_results and not version_results[version_id].empty:
                all_results[version_id].append(version_results[version_id])

        if all_results[version_id]:
            all_results[version_id] = pd.concat(all_results[version_id], ignore_index=True)
            cols_to_rename = [c for c in all_results[version_id].columns if c != 'card_id']
            all_results[version_id] = all_results[version_id].rename(
                columns={c: f"{version_id}_{c}" for c in cols_to_rename}
            )
            print(f"Completed: {len(all_results[version_id])} cards with estimates")
            sep_col = f"{version_id}_separation"
            n_sep = all_results[version_id][sep_col].sum()
            if n_sep > 0:
                print(f"WARNING: {n_sep} cards with possible separation issues")
        else:
            all_results[version_id] = pd.DataFrame()
        print()

    # Combine card results
    print("Combining results from all model versions...")
    final_results = all_results['v0']
    for version_id in list(model_versions.keys())[1:]:
        if not all_results[version_id].empty:
            final_results = final_results.merge(all_results[version_id], on='card_id', how='outer')

    card_counts = pd.Series(
        [card for cards_list in games_df['cards'] for card in cards_list]
    ).value_counts()
    game_counts_df = pd.DataFrame({
        'card_id': eligible_cards,
        'n_games': [card_counts[card] for card in eligible_cards]
    })
    final_results = final_results.merge(game_counts_df, on='card_id', how='left')

    # FDR
    print("\nApplying FDR correction and computing raw significance...")
    from statsmodels.stats.multitest import multipletests
    for version_id in model_versions.keys():
        p_col = f"{version_id}_p"
        if p_col in final_results.columns:
            _, p_adj, _, _ = multipletests(
                final_results[p_col].fillna(1), alpha=0.05, method='fdr_bh'
            )
            final_results[f"{version_id}_p_adj"] = p_adj
            final_results[f"{version_id}_significant_raw"] = final_results[p_col] < 0.05
            final_results[f"{version_id}_significant_fdr"] = p_adj < 0.05

    # Model comparison metrics
    print("Computing model comparison metrics and effect sizes...")
    final_results['best_aic'] = final_results[['v0_aic', 'v1_aic', 'v2_aic']].min(axis=1)
    final_results['aic_improvement_v0_to_v1'] = final_results['v0_aic'] - final_results['v1_aic']
    final_results['aic_improvement_v1_to_v2'] = final_results['v1_aic'] - final_results['v2_aic']
    final_results['or_range'] = (
        final_results[['v0_or', 'v1_or', 'v2_or']].max(axis=1) -
        final_results[['v0_or', 'v1_or', 'v2_or']].min(axis=1)
    )
    final_results['or_mean'] = final_results[['v0_or', 'v1_or', 'v2_or']].mean(axis=1)
    final_results['or_cv'] = final_results['or_range'] / final_results['or_mean']

    def assign_confidence(row):
        p_adj = row.get('v2_p_adj', np.nan)
        se = row.get('v2_se', np.nan)
        or_cv = row.get('or_cv', np.nan)

        if pd.isna(se):
            return 'unknown'
        if (
            p_adj < 0.05 and
            se < 0.15 and
            or_cv < 0.10
        ):
            return 'high'
        if (
            p_adj < 0.20 and
            se < 0.35 and
            or_cv < 0.25
        ):
            return 'medium'
        return 'low'

    final_results['confidence'] = final_results.apply(assign_confidence, axis=1)

    for version_id in model_versions.keys():
        prob_col = f"{version_id}_win_rate_lift_prob"
        mean_prob_col = f"{version_id}_win_rate_lift_mean_prob"
        if prob_col in final_results.columns:
            final_results[f"{version_id}_win_rate_lift_pct"] = final_results[prob_col] * 100
            final_results[f"{version_id}_win_rate_lift_mean_pct"] = final_results[mean_prob_col] * 100
            final_results[f"{version_id}_win_rate_lift_std"] = final_results[prob_col] / global_wr
            final_results[f"{version_id}_win_rate_lift_mean_std"] = final_results[mean_prob_col] / global_wr

    final_results = final_results.sort_values('v2_or', ascending=False)

    # Checkpoint
    intermediate_csv = output_csv.with_name(output_csv.stem + "_glm_only.csv")
    checkpoint = final_results
    if card_names_df is not None:
        checkpoint = final_results.merge(card_names_df, on='card_id', how='left')
        cols = ['card_id', 'card_name'] + [c for c in checkpoint.columns if c not in ('card_id', 'card_name')]
        checkpoint = checkpoint[cols]
    checkpoint.to_csv(intermediate_csv, index=False)
    print(f"\nCheckpoint saved: {intermediate_csv}")

    # Ridge
    ridge_results = fit_ridge_regression(card_matrix, games_df, eligible_cards)
    if ridge_results is not None:
        final_results = final_results.merge(ridge_results, on='card_id', how='left')

    # Archetype strength
    compute_archetype_strength(input_csv, output_csv)

    # Card names
    if card_names_df is not None and 'card_name' not in final_results.columns:
        final_results = final_results.merge(card_names_df, on='card_id', how='left')
        cols = ['card_id', 'card_name'] + [c for c in final_results.columns if c not in ('card_id', 'card_name')]
        final_results = final_results[cols]

    final_results.to_csv(output_csv, index=False)

    # --- Group analysis ---
    if card_groups is not None:
        group_matrix = create_group_matrix(games_df, card_groups, card_names_df)
        run_group_analysis(
            games_df, card_groups, group_matrix,
            model_versions, global_wr, output_csv
        )

    # --- Summary ---
    print("\n=== Analysis Complete ===")
    print(f"Total cards analyzed: {len(final_results)}")

    for version_id, version_info in model_versions.items():
        or_col = f"{version_id}_or"
        sig_raw = f"{version_id}_significant_raw"
        sig_fdr = f"{version_id}_significant_fdr"
        sep_col = f"{version_id}_separation"
        print(f"\n{version_info['name']}:")
        print(f"  Estimates: {final_results[or_col].notna().sum()}")
        print(f"  Significant (raw p < 0.05): {final_results[sig_raw].sum()}")
        print(f"  Significant (FDR < 0.05): {final_results[sig_fdr].sum()}")
        if sep_col in final_results.columns and final_results[sep_col].sum() > 0:
            print(f"  Separation warnings: {final_results[sep_col].sum()}")

    print("\n=== Top 10 Cards by Win-Rate Lift (V2 Matchup Model) ===")
    cols_v2 = ['card_id', 'card_name', 'v2_win_rate_lift_std', 'v2_win_rate_lift_pct',
               'v2_or', 'v2_p', 'v2_significant_raw', 'n_games', 'confidence']
    cols_v2 = [c for c in cols_v2 if c in final_results.columns]
    if 'v2_win_rate_lift_std' in final_results.columns:
        print(final_results.dropna(subset=['v2_win_rate_lift_std']).nlargest(10, 'v2_win_rate_lift_std')[cols_v2].to_string(index=False))

    print("\n=== Model Progression Comparison (Top 10 by games) ===")
    comp_cols = ['card_id', 'card_name', 'n_games', 'v0_or', 'v1_or', 'v2_or', 'v2_win_rate_lift_pct', 'or_cv']
    comp_cols = [c for c in comp_cols if c in final_results.columns]
    print(final_results.nlargest(10, 'n_games')[comp_cols].to_string(index=False))

    if ridge_results is not None:
        print("\n=== Ridge vs V2 Comparison (Top 10 by games) ===")
        comparison = final_results.dropna(subset=['ridge_or', 'v2_or']).copy()
        comparison['or_diff'] = (comparison['v2_or'] - comparison['ridge_or']).abs()
        ridge_cols = ['card_id', 'card_name', 'n_games', 'v2_or', 'ridge_or', 'or_diff']
        ridge_cols = [c for c in ridge_cols if c in comparison.columns]
        print(comparison.nlargest(10, 'n_games')[ridge_cols].to_string(index=False))

    print(f"\nResults saved to: {output_csv}")
    print(f"Intermediate results (GLM only): {intermediate_csv}")
    if card_groups is not None:
        group_output = output_csv.with_name(output_csv.stem + "_group_analysis.csv")
        print(f"Group analysis results: {group_output}")
    print("\nNote: Default minimum games threshold is 100.")
    print("Usage:")
    print("  python card_impact_analysis.py input.csv output.csv [min_games]")


if __name__ == "__main__":
    main()

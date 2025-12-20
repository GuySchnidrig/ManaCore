# ManaCore

---

[![Combine Datasets](https://github.com/GuySchnidrig/ManaCore/actions/workflows/data_pipeline_clean.yml/badge.svg)](https://github.com/GuySchnidrig/ManaCore/actions/workflows/data_pipeline_clean.yml)  
[![Python 3.9+](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/downloads/)  
[![License: CC BY-ND 4.0](https://img.shields.io/badge/License-CC%20BY--ND%204.0-lightgrey.svg)](https://creativecommons.org/licenses/by-nd/4.0/)  
[![GitHub last commit](https://img.shields.io/github/last-commit/GuySchnidrig/ManaCore.svg)](https://github.com/GuySchnidrig/ManaCore/commits)  
[![GitHub issues](https://img.shields.io/github/issues/GuySchnidrig/ManaCore.svg)](https://github.com/GuySchnidrig/ManaCore/issues)

---

## Overview

## Card Impact Analysis (Progressive Model Edition)
This analysis framework evaluates **individual card impact** on game outcomes using a **progressive modeling approach**, starting from a simple baseline and adding complexity in stages:

- **Version 0 (V0)**: Baseline model — `win ~ has_card + elo_diff + elo_mean`  
- **Version 1 (V1)**: Adds deck archetype — `win ~ has_card + elo_diff + elo_mean + archetype`  
- Optional: **Bayesian Hierarchical Model** for shrinkage-based card effects.

It also computes **archetype-adjusted win-rate lifts (ΔP)** and other statistics for deeper insight.

---

## Quick Start

### 1. Prepare Data
```bash
python scripts/prepare_data_for_r.py
````

**Outputs:**

* `data/processed/games_for_r.csv` – game-level observations with card lists
* `data/processed/card_lookup.csv` – mapping of card IDs to names

### 2. Run Analysis

```bash
Rscript scripts/glmer_analysis.R \
  data/processed/games_for_r.csv \
  data/processed/card_results.csv \
  100
```

**Arguments:**

* Input CSV (`games_for_r.csv`)
* Output CSV (`card_results.csv`)
* Minimum games threshold (default `100`)

**Outputs:** `card_results.csv` with card-level statistics.

---

## Model Versions

### Version 0: Baseline

* `win ~ has_card + elo_diff + elo_mean`
* Fast, simple estimate of card effect
* Produces **odds ratio (OR)**, standard error, p-values

### Version 1: + Archetype

* `win ~ has_card + elo_diff + elo_mean + archetype`
* Controls for **deck archetype**
* Computes **archetype-adjusted win-rate lift (ΔP)** for each card
* Can calculate win-rate lift at **mean covariate values** for reference

### Optional: Bayesian Hierarchical

* Requires `rstanarm`
* Model: `win ~ (1 | card_id) + elo_diff + archetype + archetype_opponent`
* Pools information across cards for **stable effect estimates**
* Outputs: Posterior OR, credible intervals, significance

---

## Key Metrics

| Metric                                      | Description                                        |
| ------------------------------------------- | -------------------------------------------------- |
| `v0_or` / `v1_or`                           | Odds ratio for having card (V0 or V1)              |
| `v1_win_rate_lift_pct`                      | ΔP, archetype-adjusted win-rate lift in %          |
| `v1_win_rate_lift_mean_pct`                 | ΔP at mean covariate values                        |
| `v*_p` / `v*_p_adj`                         | Raw and FDR-adjusted p-values                      |
| `v*_significant_raw` / `v*_significant_fdr` | Significance flags                                 |
| `n_games`                                   | Number of games card appears in                    |
| `confidence`                                | High/medium/low based on consistency & sample size |
| `bayes_or`                                  | Bayesian hierarchical odds ratio (if available)    |
| `bayes_or_lower` / `bayes_or_upper`         | 95% credible interval                              |
| `bayes_significant`                         | Credible positive/negative flag                    |

---

## Interpreting Results

* **Odds Ratios:**

  * OR > 1 → card increases win probability
  * OR < 1 → card decreases win probability

* **Win-Rate Lift (ΔP):**

  * Positive = card increases win probability, adjusted for archetype
  * Compare `v1_win_rate_lift_pct` across cards to see top performers

* **Confidence Levels:**

  * High: ≥200 games & stable OR across V0 → V1
  * Medium: ≥100 games & moderate stability
  * Low: few games or inconsistent estimates

* **Bayesian Hierarchical Estimates:**

  * Shrinks rare-card estimates toward the global mean
  * Useful for cards with limited data

---

## Example: Top Cards by ΔP

```r
final_results %>%
  arrange(desc(v1_win_rate_lift_pct)) %>%
  select(card_id, card_name, v1_win_rate_lift_pct, v1_or, n_games, confidence) %>%
  head(10)
```

---

## Model Comparison

* **AIC improvement:** Difference between V0 and V1 for model fit
* **OR stability:** Compare V0 and V1 OR to flag inconsistent effects
* **Separation warnings:** Cards with extreme coefficients indicating potential perfect prediction

---

## Troubleshooting

* **Card too rare:** Increase `min_games` or rely on Bayesian estimates
* **glmnet not available:** Ridge regularization skipped, optional
* **rstanarm not available:** Bayesian hierarchical model skipped, optional
* **Memory issues:** Filter top N cards, increase RAM

---

## Performance

| Model                 | Runtime          | Parallelizable             |
| --------------------- | ---------------- | -------------------------- |
| V0 (baseline)         | <1 sec per card  | N/A                        |
| V1 (+archetype)       | 1-2 sec per card | N/A                        |
| Bayesian Hierarchical | 10–30 min        | Partial (depends on cores) |

---


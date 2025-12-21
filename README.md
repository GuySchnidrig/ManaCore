# ManaCore


[![Combine Datasets](https://github.com/GuySchnidrig/ManaCore/actions/workflows/data_pipeline_clean.yml/badge.svg)](https://github.com/GuySchnidrig/ManaCore/actions/workflows/data_pipeline_clean.yml)  
[![Python 3.9+](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/downloads/)  
[![License: CC BY-ND 4.0](https://img.shields.io/badge/License-CC%20BY--ND%204.0-lightgrey.svg)](https://creativecommons.org/licenses/by-nd/4.0/)  
[![GitHub last commit](https://img.shields.io/github/last-commit/GuySchnidrig/ManaCore.svg)](https://github.com/GuySchnidrig/ManaCore/commits)  
[![GitHub issues](https://img.shields.io/github/issues/GuySchnidrig/ManaCore.svg)](https://github.com/GuySchnidrig/ManaCore/issues)


## Card Impact Analysis: Progressive Model Complexity

This R-based analysis evaluates **individual card impact** on game outcomes using a **progressive modeling approach**:

1. **Version 0 (V0)** – Baseline: `win ~ has_card + elo_diff + elo_mean`  
2. **Version 1 (V1)** – Adds deck archetype: `win ~ has_card + elo_diff + elo_mean + archetype`  
3. **Optional Bayesian Hierarchical Model** – Shrinks estimates for rare cards toward global mean using `rstanarm`.
4. **Optional L2-regularized logistic regression** -  across all cards simultaneously.


The framework outputs **odds ratios (OR)**, **p-values**, **archetype-adjusted win-rate lifts (ΔP)**, and other statistics for deeper insight into card effectiveness.

## Model Versions

### Version 0: Baseline

* Formula: `win ~ has_card + elo_diff + elo_mean`
* Simple, fast model estimating card effect.
* Outputs OR, standard error, and p-values.

### Version 1: + Archetype

* Formula: `win ~ has_card + elo_diff + elo_mean + archetype`
* Controls for **deck archetype**.
* Computes **archetype-adjusted win-rate lift (ΔP)** per card.
* ΔP also calculated at **mean covariate values**.

### Optional: Bayesian Hierarchical

* Requires `rstanarm`.
* Formula: `win ~ scale(elo_diff) + scale(elo_mean) + archetype + (1 + scale(elo_diff) | card_id),`.
* Pools information across cards for stable effect estimates.
* Outputs posterior OR, credible intervals, and significance flags.

---

## Key Metrics

| Metric                                      | Description                                         |
| ------------------------------------------- | --------------------------------------------------- |
| `v0_or` / `v1_or`                           | Odds ratio for having card (V0 or V1)               |
| `v1_win_rate_lift_pct`                      | Archetype-adjusted ΔP in %                          |
| `v1_win_rate_lift_mean_pct`                 | ΔP at mean covariate values                         |
| `v*_p` / `v*_p_adj`                         | Raw and FDR-adjusted p-values                       |
| `v*_significant_raw` / `v*_significant_fdr` | Significance flags                                  |
| `n_games`                                   | Number of games card appears in                     |
| `confidence`                                | High/medium/low based on sample size & OR stability |
| `bayes_or`                                  | Bayesian hierarchical OR (if available)             |
| `bayes_or_lower` / `bayes_or_upper`         | 95% credible interval                               |
| `bayes_significant`                         | Credible positive/negative flag                     |

---

## Interpreting Results

* **Odds Ratios (OR)**

  * OR > 1 → Card increases win probability
  * OR < 1 → Card decreases win probability

* **Win-Rate Lift (ΔP)**

  * Positive → Card improves win probability, adjusted for archetype
  * Compare `v1_win_rate_lift_pct` across cards to identify top performers

* **Confidence Levels**

  * High: ≥200 games & stable OR from V0 → V1
  * Medium: ≥100 games & moderate stability
  * Low: Few games or inconsistent estimates

* **Bayesian Estimates**

  * Shrinks rare-card estimates toward global mean
  * Useful for cards with limited data

---

## Model Comparison

* **AIC improvement:** Difference in model fit between V0 and V1
* **OR stability:** Compare V0 and V1 OR to flag inconsistent effects
* **Separation warnings:** Cards with extreme coefficients (potential perfect prediction)

---


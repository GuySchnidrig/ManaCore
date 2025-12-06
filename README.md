# ManaCore

[![Combine Datasets](https://github.com/GuySchnidrig/ManaCore/actions/workflows/data_pipeline_clean.yml/badge.svg)](https://github.com/GuySchnidrig/ManaCore/actions/workflows/data_pipeline_clean.yml)
[![Python 3.9+](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![License: CC BY-ND 4.0](https://img.shields.io/badge/License-CC%20BY--ND%204.0-lightgrey.svg)](https://creativecommons.org/licenses/by-nd/4.0/)
[![GitHub last commit](https://img.shields.io/github/last-commit/GuySchnidrig/ManaCore.svg)](https://github.com/GuySchnidrig/ManaCore/commits)
[![GitHub issues](https://img.shields.io/github/issues/GuySchnidrig/ManaCore.svg)](https://github.com/GuySchnidrig/ManaCore/issues)


# Card Impact Analysis - Comprehensive Guide

## Overview

This analysis suite measures the **true impact** of individual cards on game outcomes using multiple statistical approaches. It goes beyond simple win rates by controlling for confounding factors like deck quality, player skill, and matchup effects.

---

## Quick Start

### 1. Prepare Data (Python)
```bash
python scripts/prepare_data_for_r.py
```
**Output:**
- `data/processed/games_for_r.csv` - Game-level observations
- `data/processed/card_lookup.csv` - Card ID to name mapping

### 2. Run Analysis (R)
```bash
Rscript scripts/card_impact_analysis.R \
  data/processed/games_for_r.csv \
  data/processed/card_results.csv \
  30
```
**Arguments:**
- `games_for_r.csv` - Input game data
- `card_results.csv` - Output results file
- `30` - Minimum games threshold (cards must appear in ≥30 games)

**Output:** `card_results.csv` with comprehensive metrics for each card

---

## Analysis Methods

The script implements **6 complementary methods** to measure card strength:

### Method 1: Game-in-Hand Win Rate (GIH WR)
**What it measures:** Simple win rate when card is in deck

**Formula:**
```
GIH WR = Wins with Card / Total Games with Card
```

**Pros:**
- Easy to understand
- No assumptions
- Fast to compute

**Cons:**
- Confounded by deck quality (good cards appear in good decks)
- No uncertainty quantification for rare cards
- Doesn't control for matchups or player skill

**Use case:** Quick baseline metric

---

### Method 2: Bayesian GIH Win Rate
**What it measures:** Regularized win rate using Bayesian shrinkage

**Formula:**
```
Bayesian WR = (Card Wins + Prior Wins) / (Card Games + Prior Strength)
```
Where:
- Prior = Global win rate (50%)
- Prior Strength = 50 games

**How it works:**
- Cards with few games shrink toward global average (50%)
- Cards with many games rely on actual data
- Provides smooth, stable estimates

**Example:**
- Card with 3-0 record → Bayesian WR ≈ 52% (not 100%)
- Card with 300-150 record → Bayesian WR ≈ 67% (close to 66.7%)

**Pros:**
- Handles small samples elegantly
- No model convergence issues
- Still interpretable as win rate

**Cons:**
- Still confounded by deck quality
- Arbitrary choice of prior strength

**Use case:** Primary ranking metric for public-facing ratings

---

### Method 3: GLMM (Generalized Linear Mixed Model)
**What it measures:** Card effect controlling for confounders

**Model:**
```r
win ~ has_card + elo_diff + color + archetype + 
      color_opponent + archetype_opponent + (1 | deck_id)
```

**Components:**
- `has_card` - **Target effect**: Does having this card increase win probability?
- `elo_diff` - Player skill advantage
- `color` - Deck color (W, U, B, R, G, WU, etc.)
- `archetype` - Deck strategy (Aggro, Control, Midrange, etc.)
- `color_opponent` - Opponent's color
- `archetype_opponent` - Opponent's strategy
- `(1 | deck_id)` - Random effect for deck (same deck plays multiple games)

**Output: Odds Ratio (OR)**
```
OR = exp(coefficient)
```
- OR = 1.0 → No effect
- OR = 1.3 → 30% better odds of winning
- OR = 0.8 → 20% worse odds of winning

**Example Interpretation:**
> "Lightning Bolt has OR = 1.45 (95% CI: 1.32-1.58, p < 0.001)"
>
> Translation: Including Lightning Bolt in your deck increases your win odds by 45%, even when controlling for deck color, archetype, player skill, and matchup. This effect is highly statistically significant.

**Pros:**
- **Causal interpretation**: Isolates card effect from confounders
- Confidence intervals quantify uncertainty
- Can test statistical significance
- Detects when models fail (convergence warnings)

**Cons:**
- Slower (2-10 seconds per card)
- Requires minimum sample size
- Can have convergence issues with rare cards

**Use case:** Understanding which cards *cause* wins vs. just appear in winning decks

---

### Method 4: Bayesian Hierarchical Model (Optional)
**What it measures:** Pooled estimates with optimal shrinkage across all cards

**Model:**
```r
win ~ (1 | card_id) + elo_diff + color + archetype + 
      color_opponent + archetype_opponent + (1 | deck_id)
```

**Key difference from Method 3:**
- Fits **one model for all cards simultaneously**
- Card effects are random effects, not fixed effects
- Shares information across cards (hierarchical pooling)

**How shrinkage works:**
1. Estimates global card effect distribution
2. Rare cards shrink more toward the average
3. Common cards rely more on their own data
4. Shrinkage is **data-driven**, not arbitrary

**Example:**
```
Card A: 10 games, raw OR = 2.5 → Hierarchical OR = 1.4 (heavy shrinkage)
Card B: 500 games, raw OR = 2.5 → Hierarchical OR = 2.4 (minimal shrinkage)
```

**Pros:**
- Optimal bias-variance tradeoff
- More stable estimates than separate models
- Handles all cards in one framework
- Credible intervals (Bayesian uncertainty)

**Cons:**
- Slow (20-30 minutes for 300 cards)
- Requires `rstanarm` package (can be tricky to install)
- Less interpretable than individual ORs
- Can't easily test specific hypotheses per card

**Use case:** Research-grade estimates when you have time and computational resources

---

### Method 5: Stratified Analysis
**What it measures:** Context-dependency of card effects

**Computes:**
- Win rate in each archetype (Aggro, Control, etc.)
- Win rate against each opponent archetype
- Variance and range of win rates across contexts

**Metrics:**
- `context_variance` - How much WR varies across player archetypes
- `context_range` - Max WR - Min WR across contexts
- `opp_context_variance` - Variance across opponent types

**Example:**
```
Card: Wrath of God
- WR in Aggro decks: 45% (bad fit)
- WR in Control decks: 72% (excellent fit)
- Context range: 27 percentage points → HIGHLY context-dependent
```

**Use case:** Identifying cards that are archetype-specific vs. universally good

---

### Method 6: Interaction Analysis
**What it measures:** Does card effectiveness vary by player skill?

**Model:**
```r
win ~ has_card * elo_diff + color + archetype + ...
```

**Interaction term:** `has_card * elo_diff`
- Positive interaction → Better players extract more value
- Negative interaction → Card "carries" weaker players
- Zero interaction → Skill-neutral

**Example:**
```
Card: Counterspell
- Interaction coefficient: +0.0008 (p < 0.01)
- Interpretation: Requires skill to use optimally
- High-skill players gain extra 10% win probability

Card: Lightning Bolt
- Interaction coefficient: -0.0002 (p > 0.05)
- Interpretation: Skill-neutral, works for everyone
```

**Use case:** 
- Identifying "skill-testing" cards
- Adjusting ratings for different player populations
- Designing learning materials (focus on high-interaction cards)

---

## Output Columns Explained

### Basic Metrics
- `card_id` - Scryfall ID
- `card_name` - Human-readable name
- `gih_games` - Total games card appeared in
- `gih_wins` - Games won with card
- `gih_wr` - Raw win rate (Method 1)

### Bayesian Regularization
- `bayes_wr` - Regularized win rate (Method 2)
- `bayes_lift` - Difference from global win rate

### GLMM Results (Method 3)
- `glmm_coef` - Log-odds coefficient
- `glmm_se` - Standard error
- `glmm_z` - Z-statistic
- `glmm_p` - Raw p-value
- `glmm_p_adj` - FDR-adjusted p-value (use this!)
- `glmm_or` - **Odds ratio** (key metric)
- `glmm_or_lower` - 95% CI lower bound
- `glmm_or_upper` - 95% CI upper bound
- `glmm_significant` - TRUE if FDR p < 0.05
- `converged` - Model converged successfully

### Bayesian Hierarchical (Method 4, if available)
- `bayes_coef` - Posterior mean log-odds
- `bayes_se` - Posterior SD
- `bayes_or` - Posterior mean odds ratio
- `bayes_or_lower` - 95% credible interval lower
- `bayes_or_upper` - 95% credible interval upper
- `bayes_significant` - CI excludes 1.0

### Context Effects (Method 5)
- `context_variance` - Variance in WR across archetypes
- `context_range` - Range of WR across archetypes
- `n_contexts` - Number of archetypes tested
- `opp_context_variance` - Variance across opponent types
- `opp_context_range` - Range across opponent types

### Skill Interactions (Method 6)
- `interaction_coef` - Skill interaction effect
- `interaction_p` - P-value for interaction
- `skill_dependent` - Significant skill interaction detected

### Composite Scores
- `score_gih` - Normalized GIH WR (0-100)
- `score_bayes` - Normalized Bayesian WR (0-100)
- `score_glmm` - Normalized GLMM OR (0-100)
- `score_bayes_hier` - Normalized hierarchical OR (0-100)
- `composite_score` - **Weighted average** (primary ranking metric)
- `confidence` - Data quality flag (high/medium/low)

---

## Interpreting Results

### Which Metric Should I Use?

**For public card ratings (e.g., tier lists):**
→ Use `composite_score` or `bayes_wr`
- Easy to explain
- Stable for rare cards
- Incorporates multiple signals

**For understanding card power:**
→ Use `glmm_or` with `glmm_p_adj`
- Answers: "Does this card *cause* wins?"
- Controls for confounders
- Statistical significance testing

**For archetype-specific ratings:**
→ Use stratified analysis
- Check `context_range` (high = niche card)
- Compute WR per archetype separately

**For identifying skill-testing cards:**
→ Use `interaction_coef`
- Positive = rewards skill
- Use for educational content

### Understanding Odds Ratios

**OR Scale:**
- 1.50+ → Bomb/premium card
- 1.20-1.50 → Very good card
- 1.05-1.20 → Above average
- 0.95-1.05 → Neutral/filler
- 0.80-0.95 → Below average
- <0.80 → Actively bad

**Example:**
```
Card A: OR = 1.40 (CI: 1.25-1.55, p < 0.001)
→ Strong effect, tight CI, highly significant

Card B: OR = 1.40 (CI: 0.90-2.10, p = 0.12)
→ Same point estimate, but wide CI and not significant
→ Probably just noise from small sample
```

### FDR Correction

**Why it matters:**
Testing 300 cards → expect 15 false positives by chance (5%)

**FDR correction:**
- Uses Benjamini-Hochberg procedure
- Controls false discovery rate at 5%
- **Always use `glmm_p_adj`, not `glmm_p`**

**Thresholds:**
- `p_adj < 0.05` → Significant
- `p_adj < 0.01` → Highly significant
- `p_adj > 0.05` → Not enough evidence

---

## Statistical Concepts Explained

### Why Control for Confounders?

**Problem:** Good cards appear in good decks

**Example without controls:**
```
Card: Mox Ruby (only in high-ELO decks)
Raw WR: 75%
But is it the card or the player?
```

**Solution:** GLMM controls for:
1. Player skill (`elo_diff`)
2. Deck quality (`archetype`)
3. Matchup (`archetype_opponent`)
4. Repeated measures (`deck_id`)

**Result:** Isolates card effect from context

### Random Effects vs. Fixed Effects

**Fixed Effect** (Method 3):
- Estimates specific coefficient per card
- `has_card = 1` for card A, `0` otherwise
- Can test significance
- Separate model per card

**Random Effect** (Method 4):
- Estimates distribution of card effects
- All cards share variance parameter
- Pooling across cards
- Single model for all cards

**Analogy:**
- Fixed: "Tell me exactly how much each card helps"
- Random: "Tell me the typical range of card effects"

### Convergence Warnings

**What they mean:**
Model couldn't find stable solution

**Common causes:**
1. Perfect separation (card only in winning/losing decks)
2. Too rare (not enough data)
3. Collinear with archetype (only appears in one deck type)

**What to do:**
- Flag card as "insufficient data"
- Rely on Bayesian WR instead
- Collect more games

---

## Advanced Usage

### Adjusting Prior Strength (Method 2)

```r
# More aggressive shrinkage
prior_strength <- 100  # Default: 50

# Less shrinkage (trust small samples more)
prior_strength <- 20
```

### Changing Composite Score Weights

```r
# Default weights
composite_score = 0.4 * bayes_hier + 0.3 * bayes_wr + 0.2 * glmm + 0.1 * gih

# Prefer GLMM (causal focus)
composite_score = 0.5 * glmm + 0.3 * bayes_wr + 0.2 * gih

# Prefer Bayesian WR (stability focus)
composite_score = 0.7 * bayes_wr + 0.3 * gih
```

### Parallelizing GLMM (Method 3)

```r
library(parallel)

glmm_results <- mclapply(eligible_cards, function(card) {
  # Fit model...
}, mc.cores = detectCores() - 1)
```

**Speed improvement:**
- Sequential: ~10 minutes for 300 cards
- 4 cores: ~2.5 minutes

### Testing Different Interactions

```r
# Test archetype interaction
model_int <- glm(
  win ~ has_card * archetype + elo_diff + color + ...,
  family = binomial
)

# Test color interaction
model_int <- glm(
  win ~ has_card * color + elo_diff + archetype + ...,
  family = binomial
)
```

---

## Troubleshooting

### "Model failed to converge"
**Cause:** Card is too rare or perfectly predicts outcome
**Solution:** Increase `min_games` threshold or accept missing GLMM estimates

### "rstanarm not available"
**Cause:** Package installation issues
**Solution:** Script continues without Method 4 - not required

### "Memory error in long format"
**Cause:** Too many game-card observations (>10M rows)
**Solution:** 
- Increase RAM
- Filter to top N cards by frequency
- Skip Method 4

### Negative odds ratios
**Cause:** Not possible - OR is always positive
**Check:** Are you looking at log-odds (`glmm_coef`)? Use `glmm_or` instead

### All p-values are significant
**Cause:** Very large sample size (everything is "significant")
**Solution:** Focus on effect sizes (OR magnitude) not just p-values

---

## Performance Benchmarks

**Typical runtimes** (300 cards, 10,000 games):

| Method | Time | Parallelizable |
|--------|------|----------------|
| Method 1: GIH WR | <1 sec | N/A |
| Method 2: Bayesian WR | <1 sec | N/A |
| Method 3: GLMM | 10 min | Yes (→2 min) |
| Method 4: Bayesian Hierarchical | 20-30 min | No |
| Method 5: Stratified | 30 sec | Yes |
| Method 6: Interactions | 1 min | Yes |

**Total (without Method 4):** ~12 minutes
**Total (with Method 4):** ~40 minutes

---

## References & Further Reading

### Statistical Methods
- **Mixed Models:** Gelman & Hill (2006) - *Data Analysis Using Regression*
- **FDR Correction:** Benjamini & Hochberg (1995) - *Controlling the False Discovery Rate*
- **Bayesian Hierarchical Models:** Gelman et al. (2013) - *Bayesian Data Analysis*

### Game Analytics Applications
- **17Lands Magic Data:** Inspiration for GIH WR methodology
- **Hearthstone HSReplay:** Similar rating systems
- **Chess Rating Systems:** ELO adjustment methodology

### R Packages
- `lme4` - GLMM fitting
- `rstanarm` - Bayesian hierarchical models
- `dplyr` - Data manipulation
- `jsonlite` - JSON parsing

---

## Citation

If you use this analysis in research or publications:

```
Card Impact Analysis Suite (2024)
Multi-method statistical framework for measuring card strength in collectible card games
Methods: Game-in-Hand Win Rate, Bayesian Regularization, Generalized Linear Mixed Models,
Bayesian Hierarchical Models, Stratified Analysis, Interaction Testing
```

---

## Questions?

**Conceptual questions:** See "Statistical Concepts Explained" section
**Implementation issues:** Check "Troubleshooting" section
**Method choice:** See "Which Metric Should I Use?"

For additional support, review the inline code comments in `card_impact_analysis.R`
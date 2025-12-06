#!/usr/bin/env Rscript

# ============================================================================
# CARD IMPACT ANALYSIS: Multi-Method Approach
# ============================================================================
# This script compares multiple methods for measuring card strength:
# 1. Game-in-Hand Win Rate (GIH WR) - Simple baseline
# 2. Bayesian GIH WR - Regularized for small samples
# 3. GLMM Marginal Effects - Controls for confounders (with deck clustering)
# 4. Bayesian Hierarchical Model - Pooled estimation with shrinkage
# 5. Interaction Analysis - Context-dependent effects
# ============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(lme4)
  library(jsonlite)
  library(ggplot2)
})

# Try to load rstanarm, but continue if it fails
RSTANARM_AVAILABLE <- FALSE
tryCatch({
  suppressPackageStartupMessages(library(rstanarm))
  RSTANARM_AVAILABLE <- TRUE
  cat("rstanarm loaded successfully - Bayesian hierarchical model will be available\n")
}, error = function(e) {
  cat("Note: rstanarm not available - skipping Bayesian hierarchical model\n")
  cat("To enable: install.packages('rstanarm')\n")
})

args <- commandArgs(trailingOnly = TRUE)
input_csv  <- args[1]
output_csv <- args[2]
min_games  <- as.numeric(args[3])

cat("=== Comprehensive Card Impact Analysis ===\n\n")

# ------------------------------------------------------------
# Load game-level data
# ------------------------------------------------------------
games_df <- read.csv(input_csv, stringsAsFactors = FALSE)

# Parse JSON card lists
games_df$cards <- lapply(games_df$cards, function(x) {
  if (is.na(x) || trimws(x) == "") return(character(0))
  tryCatch(fromJSON(x), error=function(e) character(0))
})

cat("Loaded", nrow(games_df), "games\n")

# ------------------------------------------------------------
# Load card name lookup
# ------------------------------------------------------------
card_lookup_path <- file.path(dirname(input_csv), "card_lookup.csv")
if (file.exists(card_lookup_path)) {
  card_names_df <- read.csv(card_lookup_path, stringsAsFactors = FALSE)
  cat("Loaded card names for", nrow(card_names_df), "cards\n")
} else {
  cat("Warning: card_lookup.csv not found, card names will not be available\n")
  card_names_df <- NULL
}

# Global win rate (for Bayesian prior)
global_wr <- mean(games_df$win)
cat("Global win rate:", round(global_wr, 3), "\n\n")

# Filter cards
all_cards <- unlist(games_df$cards)
card_counts <- table(all_cards)
eligible_cards <- names(card_counts)[card_counts >= min_games]
cat("Analyzing", length(eligible_cards), "cards\n\n")

# ------------------------------------------------------------
# METHOD 1: Game-in-Hand Win Rate (GIH WR)
# ------------------------------------------------------------
cat("Method 1: Computing GIH Win Rates...\n")

gih_results <- lapply(eligible_cards, function(card) {
  has_card <- vapply(games_df$cards, function(x) card %in% x, logical(1))
  n_games <- sum(has_card)
  n_wins <- sum(games_df$win[has_card])
  wr <- n_wins / n_games
  
  data.frame(
    card_id = card,
    gih_games = n_games,
    gih_wins = n_wins,
    gih_wr = wr,
    stringsAsFactors = FALSE
  )
}) %>% bind_rows()

# ------------------------------------------------------------
# METHOD 2: Bayesian GIH Win Rate (Shrinkage Estimator)
# ------------------------------------------------------------
cat("Method 2: Computing Bayesian GIH Win Rates...\n")

# Use global win rate as prior with strength = 50 games
prior_strength <- 50
prior_wins <- global_wr * prior_strength

gih_results <- gih_results %>%
  mutate(
    bayes_wr = (gih_wins + prior_wins) / (gih_games + prior_strength),
    bayes_lift = bayes_wr - global_wr
  )

# ------------------------------------------------------------
# METHOD 3: GLMM with Controls (Deck Random Effect)
# ------------------------------------------------------------
cat("Method 3: Fitting GLMM models with controls...\n")

glmm_formula <- win ~ has_card + elo_diff + color + archetype + 
  color_opponent + archetype_opponent + (1 | deck_id)

glmm_results <- vector("list", length(eligible_cards))

for (i in seq_along(eligible_cards)) {
  if (i %% 25 == 0) cat("  Progress:", i, "/", length(eligible_cards), "\n")
  
  card <- eligible_cards[i]
  games_df$has_card <- as.numeric(vapply(
    games_df$cards, 
    function(x) card %in% x, 
    logical(1)
  ))
  
  # Convert factors
  df_model <- games_df %>%
    mutate(
      deck_id = as.factor(deck_id),
      color = as.factor(color),
      archetype = as.factor(archetype),
      color_opponent = as.factor(color_opponent),
      archetype_opponent = as.factor(archetype_opponent)
    )
  
  model <- tryCatch({
    glmer(glmm_formula, data = df_model, family = binomial,
          control = glmerControl(optimizer = "bobyqa", 
                                optCtrl = list(maxfun = 100000)))
  }, error = function(e) NULL)
  
  if (!is.null(model)) {
    coefs <- summary(model)$coefficients
    if ("has_card" %in% rownames(coefs)) {
      est <- coefs["has_card", "Estimate"]
      se <- coefs["has_card", "Std. Error"]
      z <- coefs["has_card", "z value"]
      p <- coefs["has_card", "Pr(>|z|)"]
      
      # Check convergence
      converged <- length(model@optinfo$conv$lme4) == 0
      
      glmm_results[[i]] <- data.frame(
        card_id = card,
        glmm_coef = est,
        glmm_se = se,
        glmm_z = z,
        glmm_p = p,
        glmm_or = exp(est),
        glmm_or_lower = exp(est - 1.96 * se),
        glmm_or_upper = exp(est + 1.96 * se),
        converged = converged
      )
    }
  }
}

glmm_results <- bind_rows(glmm_results)

# ------------------------------------------------------------
# METHOD 4: Bayesian Hierarchical Model (Single Pooled Model)
# ------------------------------------------------------------
bayes_results <- NULL

if (RSTANARM_AVAILABLE) {
  cat("\nMethod 4: Fitting Bayesian Hierarchical Model...\n")
  cat("This may take 10-30 minutes depending on data size.\n")
  cat("The model pools information across all cards with adaptive shrinkage.\n\n")

  # Create long-format data with card indicators
  cat("Reshaping data to long format...\n")
  game_card_long <- games_df %>%
    mutate(game_id = row_number()) %>%
    rowwise() %>%
    mutate(card_list = list(cards)) %>%
    tidyr::unnest(card_list) %>%
    rename(card_id = card_list) %>%
    ungroup()

  # Filter to eligible cards only
  game_card_long <- game_card_long %>%
    filter(card_id %in% eligible_cards)

  cat(sprintf("Long format: %d game-card observations\n", nrow(game_card_long)))
  cat(sprintf("Average cards per game: %.1f\n", 
              nrow(game_card_long) / length(unique(game_card_long$game_id))))

  # Prepare factors
  game_card_long <- game_card_long %>%
    mutate(
      card_id = as.factor(card_id),
      deck_id = as.factor(deck_id),
      color = as.factor(color),
      archetype = as.factor(archetype),
      color_opponent = as.factor(color_opponent),
      archetype_opponent = as.factor(archetype_opponent)
    )

  # Fit Bayesian hierarchical model
  bayes_model <- tryCatch({
    cat("Fitting Stan model (this will take a while)...\n")
    start_bayes <- Sys.time()
    
    model <- stan_glmer(
      win ~ (1 | card_id) + elo_diff + color + archetype + 
            color_opponent + archetype_opponent + (1 | deck_id),
      data = game_card_long,
      family = binomial(link = "logit"),
      prior = normal(0, 1, autoscale = TRUE),
      prior_covariance = decov(scale = 0.5),
      chains = 4,
      iter = 2000,
      warmup = 1000,
      cores = 4,
      seed = 42,
      adapt_delta = 0.95,
      refresh = 500
    )
    
    elapsed_bayes <- difftime(Sys.time(), start_bayes, units = "mins")
    cat(sprintf("\nBayesian model completed in %.1f minutes\n", elapsed_bayes))
    
    model
  }, error = function(e) {
    cat("Bayesian model failed:", e$message, "\n")
    cat("Continuing with other methods...\n")
    NULL
  })

  # Extract card random effects if model succeeded
  if (!is.null(bayes_model)) {
    cat("Extracting card-level estimates from Bayesian model...\n")
    
    # Get random effects (posterior means)
    card_ranefs <- ranef(bayes_model)$card_id
    
    # Get posterior standard deviations
    card_ranefs_se <- se.ranef(bayes_model)$card_id
    
    bayes_results <- data.frame(
      card_id = rownames(card_ranefs),
      bayes_coef = card_ranefs[, 1],
      bayes_se = card_ranefs_se[, 1],
      stringsAsFactors = FALSE
    ) %>%
      mutate(
        bayes_or = exp(bayes_coef),
        bayes_or_lower = exp(bayes_coef - 1.96 * bayes_se),
        bayes_or_upper = exp(bayes_coef + 1.96 * bayes_se),
        bayes_significant = (bayes_or_lower > 1.0 | bayes_or_upper < 1.0)
      )
    
    cat(sprintf("Extracted estimates for %d cards\n", nrow(bayes_results)))
    cat(sprintf("Cards with credibly positive effects: %d\n", 
                sum(bayes_results$bayes_or_lower > 1.0)))
    cat(sprintf("Cards with credibly negative effects: %d\n", 
                sum(bayes_results$bayes_or_upper < 1.0)))
  }
} else {
  cat("\nMethod 4: Bayesian Hierarchical Model - SKIPPED (rstanarm not available)\n")
}

# ------------------------------------------------------------
# METHOD 5: Stratified Analysis (Context Effects)
# ------------------------------------------------------------
cat("\nMethod 5: Computing stratified win rates...\n")

stratified_results <- lapply(eligible_cards, function(card) {
  # Create logical indicator
  has_card_logical <- vapply(games_df$cards, function(x) card %in% x, logical(1))
  
  # By archetype (player's deck)
  arch_strata <- games_df %>%
    filter(has_card_logical) %>%
    group_by(archetype) %>%
    summarise(
      n = n(),
      wr = mean(win),
      .groups = "drop"
    ) %>%
    filter(n >= 5)  # Min 5 games per stratum
  
  # By opponent archetype
  opp_arch_strata <- games_df %>%
    filter(has_card_logical) %>%
    group_by(archetype_opponent) %>%
    summarise(
      n = n(),
      wr = mean(win),
      .groups = "drop"
    ) %>%
    filter(n >= 5)
  
  if (nrow(arch_strata) > 0) {
    wr_variance <- var(arch_strata$wr)
    wr_range <- max(arch_strata$wr) - min(arch_strata$wr)
  } else {
    wr_variance <- NA
    wr_range <- NA
  }
  
  if (nrow(opp_arch_strata) > 0) {
    opp_wr_variance <- var(opp_arch_strata$wr)
    opp_wr_range <- max(opp_arch_strata$wr) - min(opp_arch_strata$wr)
  } else {
    opp_wr_variance <- NA
    opp_wr_range <- NA
  }
  
  data.frame(
    card_id = card,
    context_variance = wr_variance,
    context_range = wr_range,
    n_contexts = nrow(arch_strata),
    opp_context_variance = opp_wr_variance,
    opp_context_range = opp_wr_range
  )
}) %>% bind_rows()

# ------------------------------------------------------------
# METHOD 6: Interaction Effects (Advanced)
# ------------------------------------------------------------
cat("Method 6: Testing key interactions for top cards...\n")

# Test interaction with elo_diff for top 30 cards by GIH
top_cards_for_interaction <- gih_results %>%
  arrange(desc(gih_games)) %>%
  head(30) %>%
  pull(card_id)

interaction_results <- lapply(top_cards_for_interaction, function(card) {
  games_df$has_card <- as.numeric(vapply(
    games_df$cards, 
    function(x) card %in% x, 
    logical(1)
  ))
  
  # Convert factors
  df_model <- games_df %>%
    mutate(
      color = as.factor(color),
      archetype = as.factor(archetype),
      color_opponent = as.factor(color_opponent),
      archetype_opponent = as.factor(archetype_opponent)
    )
  
  # Model with interaction
  model_int <- tryCatch({
    glm(
      win ~ has_card * elo_diff + color + archetype + 
        color_opponent + archetype_opponent,
      data = df_model,
      family = binomial(link = "logit")
    )
  }, error = function(e) NULL)
  
  if (!is.null(model_int)) {
    coefs <- summary(model_int)$coefficients
    if ("has_card:elo_diff" %in% rownames(coefs)) {
      int_coef <- coefs["has_card:elo_diff", "Estimate"]
      int_p <- coefs["has_card:elo_diff", "Pr(>|z|)"]
      
      return(data.frame(
        card_id = card,
        interaction_coef = int_coef,
        interaction_p = int_p,
        skill_dependent = abs(int_coef) > 0.0001 && int_p < 0.05
      ))
    }
  }
  return(NULL)
}) %>% bind_rows()

# ------------------------------------------------------------
# Combine All Methods
# ------------------------------------------------------------
cat("\nCombining results from all methods...\n")

final_results <- gih_results %>%
  left_join(glmm_results, by = "card_id") %>%
  left_join(stratified_results, by = "card_id") %>%
  left_join(interaction_results, by = "card_id")

# Add Bayesian results if available
if (!is.null(bayes_results)) {
  final_results <- final_results %>%
    left_join(bayes_results, by = "card_id")
}

# Apply FDR correction to GLMM p-values
final_results <- final_results %>%
  mutate(
    glmm_p_adj = p.adjust(glmm_p, method = "BH"),
    glmm_significant = !is.na(glmm_p_adj) & glmm_p_adj < 0.05
  )

# ------------------------------------------------------------
# Create Composite Score
# ------------------------------------------------------------
cat("Creating composite card strength score...\n")

# Normalize each metric to 0-100 scale
normalize_to_100 <- function(x) {
  (x - min(x, na.rm = TRUE)) / 
    (max(x, na.rm = TRUE) - min(x, na.rm = TRUE)) * 100
}

final_results <- final_results %>%
  mutate(
    # Individual scores
    score_gih = normalize_to_100(gih_wr),
    score_bayes = normalize_to_100(bayes_wr),
    score_glmm = normalize_to_100(glmm_or),
    score_bayes_hier = if (!is.null(bayes_results)) normalize_to_100(bayes_or) else NA,
    
    # Composite: weighted average (you can adjust weights)
    # If Bayesian hierarchical available, use it; otherwise fall back to GLMM
    composite_score = case_when(
      !is.na(score_bayes_hier) ~ 0.4 * score_bayes_hier + 0.3 * score_bayes + 0.2 * score_glmm + 0.1 * score_gih,
      !is.na(score_glmm) & converged ~ 0.5 * score_glmm + 0.3 * score_bayes + 0.2 * score_gih,
      TRUE ~ 0.6 * score_bayes + 0.4 * score_gih
    ),
    
    # Confidence: penalize high variance across contexts or low sample size
    confidence = case_when(
      gih_games >= 100 & is.na(context_variance) ~ "high",
      gih_games >= 100 & context_variance < 0.01 ~ "high",
      gih_games >= 50 & context_variance < 0.02 ~ "medium",
      TRUE ~ "low"
    )
  ) %>%
  arrange(desc(composite_score))

# ------------------------------------------------------------
# Save Results (with card names)
# ------------------------------------------------------------
# Add card names if available
if (!is.null(card_names_df)) {
  final_results <- final_results %>%
    left_join(card_names_df, by = "card_id") %>%
    # Reorder columns to put card_name second
    select(card_id, card_name, everything())
}

write.csv(final_results, output_csv, row.names = FALSE)

cat("\n=== Analysis Complete ===\n")
cat("Total cards analyzed:", nrow(final_results), "\n")
cat("Cards with GLMM estimates:", sum(!is.na(final_results$glmm_or)), "\n")
cat("Converged models:", sum(final_results$converged, na.rm = TRUE), "\n")
if (!is.null(bayes_results)) {
  cat("Cards with Bayesian hierarchical estimates:", sum(!is.na(final_results$bayes_or)), "\n")
}
cat("Significant cards (FDR < 0.05):", sum(final_results$glmm_significant, na.rm = TRUE), "\n")

# ------------------------------------------------------------
# Print Top Cards by Different Metrics
# ------------------------------------------------------------
cat("\n=== Top 10 Cards by Composite Score ===\n")
cols_to_show <- c("card_id", "card_name", "composite_score", "gih_wr", "bayes_wr", "glmm_or", 
                  "gih_games", "confidence")
if (!is.null(bayes_results)) {
  cols_to_show <- c(cols_to_show[1:6], "bayes_or", cols_to_show[7:8])
}
print(final_results %>%
  select(any_of(cols_to_show)) %>%
  head(10), row.names = FALSE)

cat("\n=== Top 10 Cards by GLMM Odds Ratio (Controlled) ===\n")
print(final_results %>%
  filter(!is.na(glmm_or), converged) %>%
  arrange(desc(glmm_or)) %>%
  select(any_of(c("card_id", "card_name", "glmm_or", "glmm_or_lower", "glmm_or_upper", 
         "glmm_p_adj", "gih_games"))) %>%
  head(10), row.names = FALSE)

if (!is.null(bayes_results)) {
  cat("\n=== Top 10 Cards by Bayesian Hierarchical OR ===\n")
  print(final_results %>%
    filter(!is.na(bayes_or)) %>%
    arrange(desc(bayes_or)) %>%
    select(any_of(c("card_id", "card_name", "bayes_or", "bayes_or_lower", "bayes_or_upper", 
           "bayes_significant", "gih_games"))) %>%
    head(10), row.names = FALSE)
  
  cat("\n=== Comparison: GLMM vs Bayesian Hierarchical (Top 20 by games) ===\n")
  print(final_results %>%
    filter(!is.na(bayes_or), !is.na(glmm_or), converged) %>%
    arrange(desc(gih_games)) %>%
    mutate(
      or_diff = abs(glmm_or - bayes_or),
      shrinkage = (glmm_or - 1) / (bayes_or - 1)
    ) %>%
    select(any_of(c("card_id", "card_name", "gih_games", "glmm_or", "bayes_or", "or_diff", "shrinkage"))) %>%
    head(20), row.names = FALSE)
}

cat("\n=== Cards with Strongest Skill Interactions ===\n")
if (nrow(interaction_results) > 0) {
  print(final_results %>%
    filter(!is.na(skill_dependent), skill_dependent) %>%
    arrange(desc(abs(interaction_coef))) %>%
    select(any_of(c("card_id", "card_name", "interaction_coef", "interaction_p", "gih_wr", "glmm_or"))) %>%
    head(10), row.names = FALSE)
} else {
  cat("No significant interactions detected\n")
}

cat("\nResults saved to:", output_csv, "\n")
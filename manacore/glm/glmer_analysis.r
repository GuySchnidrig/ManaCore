#!/usr/bin/env Rscript

# =======================================================================
# CARD IMPACT ANALYSIS: Progressive Model Complexity
# =======================================================================
# This script compares models with increasing complexity:
# Version 0: win ~ has_card + elo_diff + elo_mean (baseline)
# Version 1: win ~ has_card + elo_diff + elo_mean + archetype
# =======================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(lme4)
  library(jsonlite)
})

# Try to load glmnet for regularization
GLMNET_AVAILABLE <- FALSE
tryCatch({
  suppressPackageStartupMessages(library(glmnet))
  GLMNET_AVAILABLE <- TRUE
  cat("glmnet loaded successfully - ",
      "Ridge regularization will be available\n")
}, error = function(e) {
  cat("Note: glmnet not available - ",
      "skipping ridge regularization\n")
  cat("To enable: install.packages('glmnet')\n")
})

# Try to load rstanarm, but continue if it fails
RSTANARM_AVAILABLE <- FALSE
tryCatch({
  suppressPackageStartupMessages(library(rstanarm))
  RSTANARM_AVAILABLE <- TRUE
  cat("rstanarm loaded successfully - ",
      "Bayesian hierarchical model will be available\n")
}, error = function(e) {
  cat("Note: rstanarm not available - ",
      "skipping Bayesian hierarchical model\n")
  cat("To enable: install.packages('rstanarm')\n")
})



args <- commandArgs(trailingOnly = TRUE)
input_csv  <- args[1]
output_csv <- args[2]
min_games  <- if (length(args) >= 3) as.numeric(args[3]) else 100

# Add input validation
if (!file.exists(input_csv)) {
  stop("Input file not found: ", input_csv)
}
if (min_games <= 0) {
  stop("min_games must be positive")
}

cat("=== Progressive Model Complexity Card Analysis ===\n\n")

# ---------------------------------------------------------------
# Load game-level data
# ---------------------------------------------------------------
games_df <- read.csv(input_csv, stringsAsFactors = FALSE)

# Parse JSON card lists
games_df$cards <- lapply(games_df$cards, function(x) {
  if (is.na(x) || trimws(x) == "") return(character(0))
  tryCatch(fromJSON(x), error = function(e) character(0))
})

cat("Loaded", nrow(games_df), "games\n")

# ---------------------------------------------------------------
# Load card name lookup
# ---------------------------------------------------------------
card_lookup_path <- file.path(dirname(input_csv), "card_lookup.csv")
if (file.exists(card_lookup_path)) {
  card_names_df <- read.csv(card_lookup_path, 
                           stringsAsFactors = FALSE)
  cat("Loaded card names for", nrow(card_names_df), "cards\n")
} else {
  cat("Warning: card_lookup.csv not found, ",
      "card names will not be available\n")
  card_names_df <- NULL
}

# Global win rate
global_wr <- mean(games_df$win)
cat("Global win rate:", round(global_wr, 3), "\n\n")

# Filter cards
all_cards <- unlist(games_df$cards)
card_counts <- table(all_cards)
eligible_cards <- names(card_counts)[card_counts >= min_games]
cat("Analyzing", length(eligible_cards), 
    "cards (minimum", min_games, "games)\n\n")

# ---------------------------------------------------------------
# OPTIMIZATION: Precompute card-game matrix (vectorized)
# ---------------------------------------------------------------
cat("Precomputing card-game matrix (vectorized)...\n")
card_matrix <- sapply(eligible_cards, function(card) {
  vapply(games_df$cards, function(x) card %in% x, logical(1))
})
colnames(card_matrix) <- eligible_cards
cat("Card matrix created:", nrow(card_matrix), "games x", 
    ncol(card_matrix), "cards\n\n")

# ---------------------------------------------------------------
# Progressive Model Versions (0-1)
# ---------------------------------------------------------------
cat("Fitting progressive model versions...\n\n")

model_versions <- list(
  v0 = list(
    name = "Version 0: Baseline",
    formula = win ~ has_card + elo_diff,
    type = "glm"
  ),
  v1 = list(
    name = "Version 1: + Archetype",
    formula = win ~ has_card + elo_diff + archetype,
    type = "glm"
  )
)

# Initialize results storage
all_results <- list()

for (version_id in names(model_versions)) {
  version <- model_versions[[version_id]]
  cat(sprintf("=== %s ===\n", version$name))
  cat(sprintf("Formula: %s\n\n", deparse(version$formula)))
  
  version_results <- vector("list", length(eligible_cards))
  
  for (i in seq_along(eligible_cards)) {
    if (i %% 25 == 0) {
      cat("  Progress:", i, "/", length(eligible_cards), "\n")
    }
    
    card <- eligible_cards[i]
    
    # OPTIMIZATION: Use precomputed matrix instead of recalculating
    games_df$has_card <- as.numeric(card_matrix[, card])
    
    # Prepare data with factors
    df_model <- games_df %>%
      mutate(
        archetype = as.factor(archetype)
      )
    
    # Fit model based on type
    model <- tryCatch({
      if (version$type == "glm") {
        glm(version$formula, data = df_model, family = binomial)
      } else {
        glmer(version$formula, 
              data = df_model, 
              family = binomial,
              control = glmerControl(
                optimizer = "bobyqa", 
                optCtrl = list(maxfun = 100000)
              ))
      }
    }, error = function(e) {
      cat(sprintf("  Model fitting failed for card %s: %s\n", card, e$message))
      NULL
    })
    
    if (!is.null(model)) {
      coefs <- summary(model)$coefficients
      if ("has_card" %in% rownames(coefs)) {
        est <- coefs["has_card", "Estimate"]
        se <- coefs["has_card", "Std. Error"]
        z_col <- ifelse(version$type == "glm", "z value", "z value")
        p_col <- ifelse(version$type == "glm", 
                       "Pr(>|z|)", "Pr(>|z|)")
        z <- coefs["has_card", z_col]
        p <- coefs["has_card", p_col]
        
        # Check for separation (infinite or very large coefficients)
        separation_warning <- FALSE
        if (abs(est) > 5 || se > 3) {
          separation_warning <- TRUE
          cat(sprintf(
            "  WARNING: Possible separation for card %s ",
            card
          ))
          cat(sprintf("(coef = %.2f, SE = %.2f)\n", est, se))
        }
        
        # Check convergence for mixed models
        converged <- if (version$type == "glmm") {
          length(model@optinfo$conv$lme4) == 0
        } else {
          TRUE
        }
        
        # Get AIC for model comparison
        model_aic <- AIC(model)
        
        # ========================================================
        # Compute marginal win-rate lift (ΔP)
        # This is the average treatment effect 
        # across the observed distribution
        # ========================================================
        
        win_rate_lift_prob <- NA
        win_rate_lift_mean_prob <- NA
        
        tryCatch({
          # Create two versions of the data: 
          # with and without the card
          df_with <- df_model
          df_with$has_card <- 1
          df_without <- df_model
          df_without$has_card <- 0
          
          # Predict probabilities
          p_with <- predict(model, 
                           newdata = df_with, 
                           type = "response")
          p_without <- predict(model, 
                              newdata = df_without, 
                              type = "response")
          
          # Average treatment effect (in probability units)
          win_rate_lift_prob <- mean(p_with - p_without, 
                                     na.rm = TRUE)
          
          # Also compute at mean covariate values for reference
          mean_data <- df_model %>%
            summarise(
              elo_diff = mean(elo_diff, na.rm = TRUE),
              elo_mean = mean(elo_mean, na.rm = TRUE)
            )
          
          # Add archetype if it exists in the model
          if ("archetype" %in% all.vars(as.formula(version$formula))) {
            mean_data <- mean_data %>%
              mutate(
                archetype = names(
                  sort(table(df_model$archetype), decreasing = TRUE)
                )[1]
              ) %>%
              mutate(archetype = as.factor(archetype))
          }
          
          mean_data_with <- mean_data %>% mutate(has_card = 1)
          mean_data_without <- mean_data %>% mutate(has_card = 0)
          
          p_with_mean <- predict(model, 
                                newdata = mean_data_with, 
                                type = "response")
          p_without_mean <- predict(model, 
                                   newdata = mean_data_without, 
                                   type = "response")
          win_rate_lift_mean_prob <- p_with_mean - p_without_mean
          
          # Debug output for first card
          if (i == 1) {
            cat(sprintf("  Debug for card %s (%s):\n", card, version_id))
            cat(sprintf("    win_rate_lift_prob: %.6f\n", win_rate_lift_prob))
            cat(sprintf("    win_rate_lift_mean_prob: %.6f\n", win_rate_lift_mean_prob))
          }
          
        }, error = function(e) {
          cat(sprintf("  Win rate lift calculation failed for card %s: %s\n", 
                     card, e$message))
        })
        
        version_results[[i]] <- data.frame(
          card_id = card,
          coef = est,
          se = se,
          z = z,
          p = p,
          or = exp(est),
          or_lower = exp(est - 1.96 * se),
          or_upper = exp(est + 1.96 * se),
          aic = model_aic,
          converged = converged,
          separation = separation_warning,
          win_rate_lift_prob = win_rate_lift_prob,
          win_rate_lift_mean_prob = win_rate_lift_mean_prob,
          stringsAsFactors = FALSE
        )
      }
    }
  }
  
  version_results <- bind_rows(version_results)
  
  # Add version prefix to column names
  names(version_results)[-1] <- paste0(
    version_id, "_", names(version_results)[-1]
  )
  
  all_results[[version_id]] <- version_results
  
  cat(sprintf("Completed: %d cards with estimates\n", 
              nrow(version_results)))
  if (version$type == "glmm") {
    conv_col <- paste0(version_id, "_converged")
    cat(sprintf("Converged models: %d\n", 
                sum(version_results[[conv_col]], na.rm = TRUE)))
  }
  sep_col <- paste0(version_id, "_separation")
  if (sep_col %in% names(version_results)) {
    n_separation <- sum(version_results[[sep_col]], na.rm = TRUE)
    if (n_separation > 0) {
      cat(sprintf(
        "WARNING: %d cards with possible separation issues\n", 
        n_separation
      ))
    }
  }
  cat("\n")
}

# Combine all version results
cat("Combining results from all model versions...\n")
final_results <- all_results$v0
for (version_id in names(model_versions)[-1]) {
  final_results <- final_results %>%
    left_join(all_results[[version_id]], by = "card_id")
}

# Add game counts
game_counts <- data.frame(
  card_id = eligible_cards,
  n_games = as.numeric(card_counts[eligible_cards]),
  stringsAsFactors = FALSE
)
final_results <- final_results %>%
  left_join(game_counts, by = "card_id")

# ---------------------------------------------------------------
# FDR Correction (add both raw and adjusted p-values)
# ---------------------------------------------------------------
cat("\nApplying FDR correction and computing raw ",
    "significance...\n")

for (version_id in names(model_versions)) {
  p_col <- paste0(version_id, "_p")
  p_adj_col <- paste0(version_id, "_p_adj")
  sig_raw_col <- paste0(version_id, "_significant_raw")
  sig_fdr_col <- paste0(version_id, "_significant_fdr")
  
  if (p_col %in% names(final_results)) {
    final_results <- final_results %>%
      mutate(
        !!p_adj_col := p.adjust(.data[[p_col]], method = "BH"),
        !!sig_raw_col := !is.na(.data[[p_col]]) & 
          .data[[p_col]] < 0.05,
        !!sig_fdr_col := !is.na(.data[[p_adj_col]]) & 
          .data[[p_adj_col]] < 0.05
      )
  }
}

# ---------------------------------------------------------------
# Model Comparison Metrics & Effect Sizes
# ---------------------------------------------------------------
cat("Computing model comparison metrics and effect sizes...\n")

final_results <- final_results %>%
  mutate(
    # AIC comparison (lower is better)
    best_aic = pmin(v0_aic, v1_aic, na.rm = TRUE),
    aic_improvement = v0_aic - v1_aic,
    
    # Effect size stability across V0 and V1
    or_range = pmax(v0_or, v1_or, na.rm = TRUE) - 
               pmin(v0_or, v1_or, na.rm = TRUE),
    or_cv = or_range / ((v0_or + v1_or) / 2),
    
    # Confidence based on consistency and sample size
    confidence = case_when(
      n_games >= 200 & or_cv < 0.1 ~ "high",
      n_games >= 100 & or_cv < 0.2 ~ "medium",
      TRUE ~ "low"
    )
  ) %>%
  arrange(desc(v1_or))

# IMPROVED NAMING: Add percentage and standardized versions
if ("v1_win_rate_lift_prob" %in% names(final_results)) {
  final_results <- final_results %>%
    mutate(
      # Keep probability (0-1 scale)
      v1_win_rate_lift_prob = v1_win_rate_lift_prob,
      v1_win_rate_lift_mean_prob = v1_win_rate_lift_mean_prob,
      
      # Add percentage points (0-100 scale)
      v1_win_rate_lift_pct = v1_win_rate_lift_prob * 100,
      v1_win_rate_lift_mean_pct = v1_win_rate_lift_mean_prob * 100,
      
      # STANDARDIZED: Relative to baseline win rate
      v1_win_rate_lift_std = v1_win_rate_lift_prob / global_wr,
      v1_win_rate_lift_mean_std = v1_win_rate_lift_mean_prob / 
        global_wr
    )
}

# Similar for v0 if it exists
if ("v0_win_rate_lift_prob" %in% names(final_results)) {
  final_results <- final_results %>%
    mutate(
      v0_win_rate_lift_pct = v0_win_rate_lift_prob * 100,
      v0_win_rate_lift_std = v0_win_rate_lift_prob / global_wr
    )
}

# ---------------------------------------------------------------
# CHECKPOINT: Save intermediate results before Bayesian
# ---------------------------------------------------------------
intermediate_csv <- sub("\\.csv$", "_glm_only.csv", output_csv)
if (!is.null(card_names_df)) {
  final_results_checkpoint <- final_results %>%
    left_join(card_names_df, by = "card_id") %>%
    select(card_id, card_name, everything())
} else {
  final_results_checkpoint <- final_results
}
write.csv(final_results_checkpoint, intermediate_csv, 
          row.names = FALSE)
cat(sprintf("\nCheckpoint saved: %s\n", intermediate_csv))

# ---------------------------------------------------------------
# Ridge Regression (if glmnet available)
# ---------------------------------------------------------------
ridge_results <- NULL

if (GLMNET_AVAILABLE) {
  cat("\n=== Ridge Regression (L2 Regularization) ===\n")
  cat("Fitting ridge models for all cards simultaneously...\n")
  
  tryCatch({
    # Prepare design matrix
    # Include all cards as binary features
    X <- card_matrix
    y <- games_df$win
    
    # Add control variables (elo_diff, elo_mean)
    X_controls <- cbind(
      X,
      elo_diff = games_df$elo_diff,
      elo_mean = games_df$elo_mean
    )
    
    # Fit ridge model with cross-validation to select lambda
    cat("Running cross-validation to select lambda...\n")
    ridge_cv <- cv.glmnet(
      X_controls, 
      y, 
      family = "binomial",
      alpha = 0,  # Ridge (L2)
      nfolds = 5,
      type.measure = "deviance"
    )
    
    # Extract coefficients at optimal lambda
    ridge_coefs <- coef(ridge_cv, s = "lambda.min")
    
    # Get card coefficients (exclude intercept and controls)
    card_indices <- 1:ncol(X)
    ridge_card_coefs <- ridge_coefs[card_indices + 1]  # +1 for intercept
    
    ridge_results <- data.frame(
      card_id = eligible_cards,
      ridge_coef = as.numeric(ridge_card_coefs),
      ridge_or = exp(as.numeric(ridge_card_coefs)),
      ridge_lambda_min = ridge_cv$lambda.min,
      stringsAsFactors = FALSE
    )
    
    cat(sprintf("Ridge regression completed for %d cards\n", 
                nrow(ridge_results)))
    cat(sprintf("Optimal lambda: %.6f\n", ridge_cv$lambda.min))
    
  }, error = function(e) {
    cat("Ridge regression failed:", e$message, "\n")
    ridge_results <- NULL
  })
}

# Add ridge results if available
if (!is.null(ridge_results)) {
  final_results <- final_results %>%
    left_join(ridge_results, by = "card_id")
}

# ---------------------------------------------------------------
# Bayesian Hierarchical Model (ALL CARDS - no minimum filter)
# ---------------------------------------------------------------
bayes_results <- NULL

if (RSTANARM_AVAILABLE) {
  cat("\n=== Bayesian Hierarchical Model (ALL CARDS) ===\n")
  cat("Note: Including ALL cards regardless of minimum games threshold\n")
  cat("Hierarchical model will shrink estimates for rare cards toward mean\n")
  cat("This may take 10-30 minutes depending on data size.\n\n")

  # Get ALL unique cards (not just eligible_cards)
  all_unique_cards <- unique(unlist(games_df$cards))
  all_unique_cards <- all_unique_cards[!is.na(all_unique_cards) & 
                                       all_unique_cards != ""]
  
  cat(sprintf("Bayesian model will include %d total cards\n", 
              length(all_unique_cards)))
  cat(sprintf("  - %d cards meet minimum threshold (%d games)\n", 
              length(eligible_cards), min_games))
  cat(sprintf("  - %d additional rare cards included\n", 
              length(all_unique_cards) - length(eligible_cards)))

  # Create long-format data with ALL cards
  cat("\nReshaping data to long format...\n")
  game_card_long <- games_df %>%
    mutate(game_id = row_number()) %>%
    rowwise() %>%
    mutate(card_list = list(cards)) %>%
    tidyr::unnest(card_list) %>%
    rename(card_id = card_list) %>%
    ungroup()

  # Filter to non-empty cards but DON'T filter by eligible_cards
  game_card_long <- game_card_long %>%
    filter(!is.na(card_id), card_id != "", card_id %in% all_unique_cards)

  cat(sprintf("Long format: %d game-card observations\n", 
              nrow(game_card_long)))

  # Prepare factors
  game_card_long <- game_card_long %>%
    mutate(
      card_id = as.factor(card_id),
      deck_id = as.factor(deck_id),
      archetype = as.factor(archetype),
      archetype_opponent = as.factor(archetype_opponent)
    )

  # Show distribution of cards by game count
  card_game_counts <- game_card_long %>%
    group_by(card_id) %>%
    summarise(n = n(), .groups = "drop")
  
  cat("\nCard distribution:\n")
  cat(sprintf("  Cards with 1-9 games: %d\n", 
              sum(card_game_counts$n < 10)))
  cat(sprintf("  Cards with 10-49 games: %d\n", 
              sum(card_game_counts$n >= 10 & card_game_counts$n < 50)))
  cat(sprintf("  Cards with 50-99 games: %d\n", 
              sum(card_game_counts$n >= 50 & card_game_counts$n < 100)))
  cat(sprintf("  Cards with 100+ games: %d\n", 
              sum(card_game_counts$n >= 100)))

  # Fit Bayesian model
  bayes_model <- tryCatch({
    cat("\nFitting Stan model...\n")
    start_bayes <- Sys.time()
    
    model <- stan_glmer(
      win ~ 
      scale(elo_diff) + 
      scale(elo_mean) + 
      archetype + 
      (1 + scale(elo_diff) | card_id),
      data = game_card_long,
      family = binomial(link = "logit"),
      # Loosen from 0.7 to 1.0 - allows larger fixed effects
      prior = normal(0, 1.0, autoscale = TRUE),
      # Loosen from 0.5 to 1.0 - allows more card-to-card variation
      prior_covariance = decov(scale = 1.0),
      chains = 4,
      iter = 2000,
      warmup = 1000,
      cores = 2,
      seed = 42,
      adapt_delta = 0.95,
      refresh = 5
    )
    
    elapsed_bayes <- difftime(Sys.time(), 
                             start_bayes, 
                             units = "mins")
    cat(sprintf("\nBayesian model completed in %.1f minutes\n", 
                elapsed_bayes))
    
    model
  }, error = function(e) {
    cat("Bayesian model failed:", e$message, "\n")
    NULL
  })

  # Extract card-level estimates from Stan posterior
  if (!is.null(bayes_model)) {
    cat("Extracting card-level estimates...\n")
    
    tryCatch({
      # Extract posterior draws using rstanarm's method
      posterior_draws <- as.matrix(bayes_model)
      
      # Get column names for card random effects
      # Pattern: b[(Intercept) card_id:CARDNAME]
      card_cols <- grep(
        "^b\\[\\(Intercept\\) card_id:", 
        colnames(posterior_draws), 
        value = TRUE
      )
      
      if (length(card_cols) > 0) {
        # Extract card IDs from column names
        card_ids <- gsub(
          "^b\\[\\(Intercept\\) card_id:(.+)\\]$", 
          "\\1", 
          card_cols
        )
        
        # Calculate posterior summaries
        bayes_results <- data.frame(
          card_id = card_ids,
          bayes_coef = colMeans(posterior_draws[, card_cols]),
          bayes_se = apply(posterior_draws[, card_cols], 2, sd),
          stringsAsFactors = FALSE
        )
        
        # Add game counts for ALL cards in Bayesian model
        bayes_game_counts <- game_card_long %>%
          group_by(card_id) %>%
          summarise(bayes_n_games = n(), .groups = "drop") %>%
          mutate(card_id = as.character(card_id))
        
        bayes_results <- bayes_results %>%
          left_join(bayes_game_counts, by = "card_id")
        
        # Add credible intervals
        bayes_results <- bayes_results %>%
          mutate(
            bayes_or = exp(bayes_coef),
            bayes_or_lower = exp(bayes_coef - 1.96 * bayes_se),
            bayes_or_upper = exp(bayes_coef + 1.96 * bayes_se),
            bayes_significant = (
              bayes_or_lower > 1.0 | bayes_or_upper < 1.0
            ),
            # Flag cards that were included only in Bayesian model
            bayes_only = !(card_id %in% eligible_cards),
            # Measure of shrinkage (SE relative to coefficient)
            bayes_shrinkage_ratio = bayes_se / abs(bayes_coef)
          )
        
        cat(sprintf("Extracted estimates for %d cards\n", 
                    nrow(bayes_results)))
        cat(sprintf("  - %d cards from main analysis\n",
                    sum(!bayes_results$bayes_only)))
        cat(sprintf("  - %d cards only in Bayesian model (< %d games)\n",
                    sum(bayes_results$bayes_only), min_games))
      } else {
        cat("Warning: Could not find card random effects ",
            "in posterior\n")
        cat("Available parameter names (first 20):\n")
        print(head(colnames(posterior_draws), 20))
        bayes_results <- NULL
      }
    }, error = function(e) {
      cat("Error extracting Bayesian results:", e$message, "\n")
      bayes_results <- NULL
    })
  }
}

# Add Bayesian results if available
# This will now include cards not in final_results
if (!is.null(bayes_results)) {
  # For cards in main analysis, left join as before
  final_results <- final_results %>%
    left_join(
      bayes_results %>% filter(!bayes_only) %>% select(-bayes_only),
      by = "card_id"
    )
  
  # Create separate dataframe for Bayesian-only cards
  bayes_only_results <- bayes_results %>%
    filter(bayes_only)
  
  if (nrow(bayes_only_results) > 0) {
    cat(sprintf("\nCreating supplementary results for %d rare cards...\n",
                nrow(bayes_only_results)))
    
    # Add card names if available
    if (!is.null(card_names_df)) {
      bayes_only_results <- bayes_only_results %>%
        left_join(card_names_df, by = "card_id") %>%
        select(card_id, card_name, everything())
    }
    
    # Save separate file for Bayesian-only results
    bayes_only_csv <- sub("\\.csv$", "_bayes_rare_cards.csv", output_csv)
    write.csv(bayes_only_results, bayes_only_csv, row.names = FALSE)
    cat(sprintf("Rare cards (Bayesian only) saved to: %s\n", 
                bayes_only_csv))
  }
}

# ---------------------------------------------------------------
# Save Final Results
# ---------------------------------------------------------------
if (!is.null(card_names_df)) {
  final_results <- final_results %>%
    left_join(card_names_df, by = "card_id") %>%
    select(card_id, card_name, everything())
}

write.csv(final_results, output_csv, row.names = FALSE)

# ---------------------------------------------------------------
# Summary Statistics
# ---------------------------------------------------------------
cat("\n=== Analysis Complete ===\n")
cat("Total cards analyzed:", nrow(final_results), "\n")
for (version_id in names(model_versions)) {
  or_col <- paste0(version_id, "_or")
  conv_col <- paste0(version_id, "_converged")
  
  cat(sprintf("\n%s:\n", model_versions[[version_id]]$name))
  cat(sprintf("  Estimates: %d\n", 
              sum(!is.na(final_results[[or_col]]))))
  if (conv_col %in% names(final_results)) {
    cat(sprintf("  Converged: %d\n", 
                sum(final_results[[conv_col]], na.rm = TRUE)))
  }
  sig_raw_col <- paste0(version_id, "_significant_raw")
  sig_fdr_col <- paste0(version_id, "_significant_fdr")
  cat(sprintf("  Significant (raw p < 0.05): %d\n", 
              sum(final_results[[sig_raw_col]], na.rm = TRUE)))
  cat(sprintf("  Significant (FDR < 0.05): %d\n", 
              sum(final_results[[sig_fdr_col]], na.rm = TRUE)))
  sep_col <- paste0(version_id, "_separation")
  if (sep_col %in% names(final_results)) {
    n_sep <- sum(final_results[[sep_col]], na.rm = TRUE)
    if (n_sep > 0) {
      cat(sprintf("  Separation warnings: %d\n", n_sep))
    }
  }
}

if (!is.null(ridge_results)) {
  cat(sprintf("\nRidge Regression:\n"))
  cat(sprintf("  Estimates: %d\n", 
              sum(!is.na(final_results$ridge_or))))
  cat(sprintf("  Mean abs coefficient: %.4f\n", 
              mean(abs(final_results$ridge_coef), na.rm = TRUE)))
}

if (!is.null(bayes_results)) {
  cat(sprintf("\nBayesian Hierarchical:\n"))
  cat(sprintf("  Estimates: %d\n", 
              sum(!is.na(final_results$bayes_or))))
  cat(sprintf("  Credibly positive: %d\n", 
              sum(final_results$bayes_or_lower > 1.0, 
                  na.rm = TRUE)))
  cat(sprintf("  Credibly negative: %d\n", 
              sum(final_results$bayes_or_upper < 1.0, 
                  na.rm = TRUE)))
}

# ---------------------------------------------------------------
# Top Cards by Different Metrics
# ---------------------------------------------------------------
cat("\n=== Top 10 Cards by Win-Rate Lift ",
    "(V1 Archetype-Adjusted) ===\n")
cat("(Standardized by baseline win rate)\n")
cols_lift <- c("card_id", "card_name", 
               "v1_win_rate_lift_std", 
               "v1_win_rate_lift_pct", 
               "v1_or", "v1_p", "v1_significant_raw", 
               "n_games", "confidence")
print(final_results %>%
  filter(!is.na(v1_win_rate_lift_std)) %>%
  arrange(desc(v1_win_rate_lift_std)) %>%
  select(any_of(cols_lift)) %>%
  head(10), row.names = FALSE)

cat("\n=== Top 10 Cards by Version 1 Odds Ratio ===\n")
cols_to_show <- c("card_id", "card_name", "v1_or", 
                  "v1_or_lower", "v1_or_upper", 
                  "v1_win_rate_lift_pct", "v1_p", 
                  "v1_significant_raw", "n_games", "confidence")
print(final_results %>%
  filter(!is.na(v1_or)) %>%
  arrange(desc(v1_or)) %>%
  select(any_of(cols_to_show)) %>%
  head(10), row.names = FALSE)

cat("\n=== Model Progression Comparison (Top 10 by games) ===\n")
comparison_cols <- c("card_id", "card_name", "n_games", 
                     "v0_or", "v1_or", 
                     "v1_win_rate_lift_pct", "or_cv")
print(final_results %>%
  arrange(desc(n_games)) %>%
  select(any_of(comparison_cols)) %>%
  head(10), row.names = FALSE)

if (!is.null(ridge_results)) {
  cat("\n=== Ridge vs V1 Comparison (Top 10 by games) ===\n")
  print(final_results %>%
    filter(!is.na(ridge_or), !is.na(v1_or)) %>%
    arrange(desc(n_games)) %>%
    mutate(
      or_diff = abs(v1_or - ridge_or),
      shrinkage_pct = if("v1_coef" %in% names(.)) {
        100 * (1 - abs(ridge_coef) / abs(v1_coef))
      } else {
        100 * (1 - abs(ridge_coef) / abs(log(v1_or)))
      }
    ) %>%
    select(any_of(c("card_id", "card_name", "n_games", 
                    "v1_or", "ridge_or", 
                    "or_diff", "shrinkage_pct"))) %>%
    head(10), row.names = FALSE)
}

if (!is.null(bayes_results)) {
  cat("\n=== Bayesian Hierarchical vs V1 Comparison ",
      "(Top 10 by games) ===\n")
  print(final_results %>%
    filter(!is.na(bayes_or), !is.na(v1_or)) %>%
    arrange(desc(n_games)) %>%
    mutate(
      or_diff = abs(v1_or - bayes_or),
      shrinkage = (v1_or - 1) / (bayes_or - 1)
    ) %>%
    select(any_of(c("card_id", "card_name", "n_games", 
                    "v1_or", "bayes_or", 
                    "or_diff", "shrinkage"))) %>%
    head(10), row.names = FALSE)
}

cat("\nResults saved to:", output_csv, "\n")
cat("Intermediate results (GLM only) saved to:", 
    intermediate_csv, "\n")
cat("\nNote: Default minimum games threshold is 100. ",
    "Adjust with:\n")
cat("  Rscript glmer_analysis.R input.csv output.csv ",
    "[min_games]\n")
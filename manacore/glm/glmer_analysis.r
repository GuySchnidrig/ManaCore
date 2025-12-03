#!/usr/bin/env Rscript
# glmer_analysis.R
# Pure R script for running GLMER models on card data

# Load required libraries
suppressPackageStartupMessages({
  library(lme4)
  library(lmerTest)
  library(dplyr)
  library(readr)
})

cat("========================================\n")
cat("GLMER Analysis in R\n")
cat("========================================\n\n")

# Parse command line arguments
args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2) {
  stop("Usage: Rscript glmer_analysis.R <input_games.csv> <output_results.csv>")
}

input_file <- args[1]
output_file <- args[2]
min_games <- ifelse(length(args) >= 3, as.integer(args[3]), 3)

cat(sprintf("Input file: %s\n", input_file))
cat(sprintf("Output file: %s\n", output_file))
cat(sprintf("Min games with card: %d\n\n", min_games))

# Load game-level data prepared by Python
cat("Loading data...\n")
games_df <- read_csv(input_file, show_col_types = FALSE)

cat(sprintf("Loaded %d game observations\n", nrow(games_df)))
cat(sprintf("Unique players: %d\n", length(unique(games_df$player_name))))
cat(sprintf("Unique cards: %d\n", length(unique(games_df$card_id))))
cat(sprintf("Win rate: %.3f\n\n", mean(games_df$win)))

# Standardize continuous predictors
standardize <- function(x) {
  (x - mean(x, na.rm = TRUE)) / sd(x, na.rm = TRUE)
}

games_df <- games_df %>%
  mutate(
    elo_diff_c = standardize(elo_diff),
    elo_mean_c = standardize(elo_mean)
  )

# Get unique cards to analyze
card_counts <- games_df %>%
  filter(has_card == 1) %>%
  group_by(card_id, card_name) %>%
  summarize(
    n_games = n(),
    n_decks = n_distinct(deck_id),
    .groups = 'drop'
  ) %>%
  filter(n_games >= min_games)

cat(sprintf("Analyzing %d cards (with %d+ games)\n\n", nrow(card_counts), min_games))

# Function to fit GLMER for a single card
fit_card_model <- function(card_id, card_name, games_df) {
  # Filter to games where we can compare with/without the card
  card_games <- games_df %>%
    filter(card_id == !!card_id | has_card == 0)
  
  # Create indicator for THIS specific card
  card_games$has_this_card <- ifelse(card_games$card_id == card_id, 1, 0)
  
  # Fit GLMER with random intercept for player
  tryCatch({
    # Formula with archetype interactions
    model <- glmer(
      win ~ elo_diff_c + elo_mean_c + color + has_this_card + 
            archetype * archetype_opponent + (1 | player_name),
      data = card_games,
      family = binomial,
      control = glmerControl(optimizer = "bobyqa", optCtrl = list(maxfun = 2e5))
    )
    
    # Extract coefficient for the card
    coef_summary <- summary(model)$coefficients
    card_row <- coef_summary[rownames(coef_summary) == "has_this_card", , drop = FALSE]
    
    if (nrow(card_row) == 0) {
      return(NULL)
    }
    
    coef_val <- card_row[1, "Estimate"]
    std_err <- card_row[1, "Std. Error"]
    z_val <- card_row[1, "z value"]
    p_val <- card_row[1, "Pr(>|z|)"]
    
    # Calculate descriptive stats
    win_rate_with <- mean(card_games$win[card_games$has_this_card == 1])
    win_rate_without <- mean(card_games$win[card_games$has_this_card == 0])
    
    # Return results
    data.frame(
      card_id = card_id,
      card_name = card_name,
      coefficient = coef_val,
      std_error = std_err,
      z_value = z_val,
      p_value = p_val,
      odds_ratio = exp(coef_val),
      games_with_card = sum(card_games$has_this_card == 1),
      total_games = nrow(card_games),
      win_rate_with = win_rate_with,
      win_rate_without = win_rate_without,
      convergence = "success",
      stringsAsFactors = FALSE
    )
  }, error = function(e) {
    # Return error info
    data.frame(
      card_id = card_id,
      card_name = card_name,
      coefficient = NA,
      std_error = NA,
      z_value = NA,
      p_value = NA,
      odds_ratio = NA,
      games_with_card = NA,
      total_games = NA,
      win_rate_with = NA,
      win_rate_without = NA,
      convergence = paste("error:", substr(e$message, 1, 100)),
      stringsAsFactors = FALSE
    )
  })
}

# Fit models for all cards
cat("Fitting GLMER models...\n")
results_list <- list()

for (i in 1:nrow(card_counts)) {
  if (i %% 10 == 0 || i == 1) {
    cat(sprintf("Progress: %d/%d\n", i, nrow(card_counts)))
  }
  
  card_id <- card_counts$card_id[i]
  card_name <- card_counts$card_name[i]
  
  result <- fit_card_model(card_id, card_name, games_df)
  
  if (!is.null(result)) {
    results_list[[i]] <- result
  }
}

# Combine all results
results_df <- bind_rows(results_list)

# Filter to successful fits
successful_results <- results_df %>%
  filter(convergence == "success")

cat(sprintf("\nSuccessfully fit models for %d/%d cards\n", 
            nrow(successful_results), nrow(card_counts)))

if (nrow(successful_results) > 0) {
  # FDR correction
  successful_results <- successful_results %>%
    mutate(
      fdr_p = p.adjust(p_value, method = "BH"),
      significant = fdr_p < 0.05
    ) %>%
    arrange(desc(coefficient))
  
  # Save results
  write_csv(successful_results, output_file)
  cat(sprintf("\nResults saved to: %s\n", output_file))
  
  # Print summary
  cat("\n========================================\n")
  cat("SUMMARY\n")
  cat("========================================\n")
  cat(sprintf("Total cards analyzed: %d\n", nrow(successful_results)))
  cat(sprintf("Significant cards (FDR < 0.05): %d\n", sum(successful_results$significant)))
  cat(sprintf("Coefficient range: %.3f to %.3f\n", 
              min(successful_results$coefficient), 
              max(successful_results$coefficient)))
  
  cat("\n--- Top 10 Cards (Highest Win Impact) ---\n")
  top_cards <- head(successful_results, 10)
  print(select(top_cards, card_name, coefficient, p_value, odds_ratio), n = 10)
  
  cat("\n--- Bottom 10 Cards (Lowest Win Impact) ---\n")
  bottom_cards <- tail(successful_results, 10)
  print(select(bottom_cards, card_name, coefficient, p_value, odds_ratio), n = 10)
  
} else {
  cat("\nNo successful model fits. Check your data.\n")
}

cat("\n========================================\n")
cat("Analysis complete!\n")
cat("========================================\n")
#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(dplyr)
  library(lme4)
  library(jsonlite)
})

args <- commandArgs(trailingOnly = TRUE)
input_csv  <- args[1]
output_csv <- args[2]
min_games  <- as.numeric(args[3])

cat("=== GLMER Analysis (Game-level) ===\n")
cat("Input:", input_csv, "\n")
cat("Output:", output_csv, "\n")
cat("Min games:", min_games, "\n\n")

# ------------------------------------------------------------
# Load game-level data
# ------------------------------------------------------------

games_df <- read.csv(input_csv, stringsAsFactors = FALSE)

# Ensure essential columns exist
required_cols <- c("win","player_elo","opponent_elo","elo_diff","elo_mean",
                   "color","archetype","draft_id","deck_id","cards")

missing_cols <- setdiff(required_cols, colnames(games_df))
if (length(missing_cols) > 0) {
  stop("Missing required columns: ", paste(missing_cols, collapse=", "))
}

# Parse JSON card lists
games_df$cards <- lapply(games_df$cards, function(x) {
  # Safety: empty or NA → empty list
  if (is.na(x) || trimws(x) == "") return(character(0))
  tryCatch(fromJSON(x), error=function(e) character(0))
})

cat("Loaded", nrow(games_df), "game observations\n")

# ------------------------------------------------------------
# Build card universe
# ------------------------------------------------------------

all_cards <- unique(unlist(games_df$cards))
cat("Found", length(all_cards), "unique cards\n\n")

card_lookup <- data.frame(
  card_id   = all_cards,
  card_name = all_cards,   # placeholder (replace if we have names)
  stringsAsFactors = FALSE
)

# ------------------------------------------------------------
# Results storage
# ------------------------------------------------------------

results <- list()

# ------------------------------------------------------------
# GLMM formula (safe)
# ------------------------------------------------------------

glmm_formula <- win ~ has_card + elo_diff + color + archetype +
  (1 | draft_id) + (1 | deck_id)

# ------------------------------------------------------------
# MAIN CARD LOOP
# ------------------------------------------------------------

for (i in seq_len(nrow(card_lookup))) {

  if (i %% 25 == 0)
    cat("Processing", i, "of", nrow(card_lookup), "\n")

  card_id <- card_lookup$card_id[i]
  card_name <- card_lookup$card_name[i]

  # Compute has_card fast
  games_df$has_card <- vapply(
    games_df$cards,
    function(x) card_id %in% x,
    logical(1)
  )

  n_games <- sum(games_df$has_card)

  # Skip low-usage cards
  if (n_games < min_games)
    next

  # Subset + convert logical → numeric
  df_sub <- games_df %>%
    mutate(
      has_card = as.numeric(has_card),
      deck_id = as.factor(deck_id),
      draft_id = as.factor(draft_id),
      color = as.factor(color),
      archetype = as.factor(archetype)
    )

  # Fit GLMM safely
  model <- tryCatch({
    glmer(
      glmm_formula,
      data = df_sub,
      family = binomial(link = "logit"),
      control = glmerControl(optimizer = "bobyqa",
                             optCtrl = list(maxfun = 200000))
    )
  }, error = function(e) {
    message("Model failed for card ", card_id, ": ", e$message)
    NULL
  })

  if (is.null(model))
    next

  coefs <- summary(model)$coefficients

  if (!("has_card" %in% rownames(coefs))) {
    message("Coefficient missing for card ", card_id)
    next
  }

  est <- coefs["has_card", "Estimate"]
  p <- coefs["has_card", "Pr(>|z|)"]

  results[[length(results) + 1]] <- data.frame(
    card_id = card_id,
    card_name = card_name,
    n_games = n_games,
    estimate = est,
    p_value = p,
    stringsAsFactors = FALSE
  )
}

# ------------------------------------------------------------
# Save results
# ------------------------------------------------------------

results_df <- bind_rows(results)

write.csv(results_df, output_csv, row.names = FALSE)

cat("\nAnalysis complete.\n")
cat("Saved:", output_csv, "\n")

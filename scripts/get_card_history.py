from manacore.cardbase import get_cardbase
from manacore.config.get_seasons import load_season_config, load_season_start_dates

# Load full season data from JSON
cube_seasons = load_season_config()
    
season_start_dates = load_season_start_dates()

# Load and process all history
posts_with_season = get_cardbase.load_combined_history_data('data/processed/history')
history_records = get_cardbase.process_history_entries(posts_with_season)

# Export to CSV

get_cardbase.export_history_csv(history_records)


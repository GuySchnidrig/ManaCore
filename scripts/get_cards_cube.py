from manacore.cardbase import get_cardbase
from manacore.config.get_seasons import load_season_config, load_season_start_dates

# Load full season data from JSON
cube_seasons = load_season_config()

# Load derived start dates
season_start_dates = load_season_start_dates()

# Fetch mainboard snapshot
mainboard_data = get_cardbase.fetch_cube_mainboard(cube_seasons, season_start_dates)

# Export to CSV
get_cardbase.export_mainboard_csv(mainboard_data)

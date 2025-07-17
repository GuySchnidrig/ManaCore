import json
from datetime import datetime

def load_season_config(path="manacore/config/seasons.json"):
    with open(path, "r") as f:
        return  json.load(f)
    
def load_season_start_dates(filepath='manacore/config/seasons.json'):
    """Load season start dates from a JSON season map."""
    season_data = load_season_config()
    
    season_start_dates = {}
    for season, data in season_data.items():
        date_range = data["date_range"]
        start_str, _ = date_range.split('-')
        start_date = datetime.strptime(start_str, "%Y%m%d").date().isoformat()
        season_start_dates[season] = start_date

    return season_start_dates

def get_season_for_date(date_str, season_config):
    # date_str is YYYYMMDD string
    date_int = int(date_str)

    for season_name, data in season_config.items():
        date_range = data["date_range"]
        start_str, end_str = date_range.split('-')
        start_int, end_int = int(start_str), int(end_str)

        if start_int <= date_int <= end_int:
            return season_name

    return "Unknown Season"

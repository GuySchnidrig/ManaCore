import json
from datetime import datetime

def load_season_config(path="manacore/config/seasons.json"):
    """
    Load season configuration from a JSON file.

    Parameters:
    -----------
    path : str, optional
        File path to the JSON season configuration (default is "manacore/config/seasons.json").

    Returns:
    --------
    dict
        Parsed JSON data mapping season names to their metadata, including date ranges.
    """
    with open(path, "r") as f:
        return  json.load(f)
    
def load_season_start_dates(filepath='manacore/config/seasons.json'):
    """
    Load the start dates of seasons from a JSON season configuration file.

    Parameters:
    -----------
    filepath : str, optional
        Path to the JSON season configuration file (default is 'manacore/config/seasons.json').

    Returns:
    --------
    dict
        Dictionary mapping season names to their start dates (ISO format strings, YYYY-MM-DD).
    """
    season_data = load_season_config()
    
    season_start_dates = {}
    for season, data in season_data.items():
        date_range = data["date_range"]
        start_str, _ = date_range.split('-')
        start_date = datetime.strptime(start_str, "%Y%m%d").date().isoformat()
        season_start_dates[season] = start_date

    return season_start_dates

def get_season_for_date(date_input, season_config):
    """
    Determine the season name for a given date using the season configuration.

    Parameters:
    -----------
    date_input : str or datetime.date or datetime.datetime
        Date to find the corresponding season for. Can be a string in 'YYYYMMDD' format or a date/datetime object.
    
    season_config : dict
        Season configuration mapping season names to their date ranges.

    Returns:
    --------
    str
        The season name corresponding to the given date. Returns "Unknown Season" if no match is found.
    """
    # Accept either a string YYYYMMDD or a datetime.date object
    if isinstance(date_input, str):
        date_int = int(date_input)
    else:
        # date_input is datetime.date or datetime.datetime
        date_int = int(date_input.strftime("%Y%m%d"))

    for season_name, data in season_config.items():
        start_str, end_str = data["date_range"].split('-')
        start_int, end_int = int(start_str), int(end_str)

        if start_int <= date_int <= end_int:
            return season_name

    return "Unknown Season"
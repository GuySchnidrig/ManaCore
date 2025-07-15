import os
import zipfile
import pandas as pd
from pathlib import Path
import re
import json

def extract_date_from_filename(filename: str) -> str:
    # Extract date in YYYY_MM_DD format from filename, return YYYYMMDD string
    match = re.search(r'(\d{4})_(\d{2})_(\d{2})', filename)
    if match:
        return f"{match.group(1)}{match.group(2)}{match.group(3)}"
    return "unknown_date"

def load_season_config(path="manacore/config/seasons.json"):
    with open(path, "r") as f:
        return json.load(f)

def get_season_for_date(date_str, season_config):
    # date_str is YYYYMMDD string
    date_int = int(date_str)
    for date_range, season_name in season_config.items():
        start_str, end_str = date_range.split('-')
        start_int, end_int = int(start_str), int(end_str)
        if start_int <= date_int <= end_int:
            return season_name
    return "Unknown Season"

def read_all_csvs_from_zips():
    base_path = "data/raw"
    zip_files = [os.path.join(base_path, f) for f in os.listdir(base_path) if f.endswith('.zip')]

    drafted_dfs = []
    matches_dfs = []
    season_config = load_season_config()

    for zip_file in zip_files:
        date_str = extract_date_from_filename(os.path.basename(zip_file))  # e.g. "20250609"
        with zipfile.ZipFile(zip_file, 'r') as z:
            # Find drafted decks CSVs
            drafted_decks_csvs = [name for name in z.namelist() if 'drafted_decks' in name.lower()]
            # Find matches CSVs
            matches_csvs = [name for name in z.namelist() if 'matches' in name.lower()]

            for drafted_csv in drafted_decks_csvs:
                with z.open(drafted_csv) as f:
                    df = pd.read_csv(f)
                    df['draft_id'] = date_str  # add draft_id column here
                    df['season_id'] = get_season_for_date(date_str, season_config)
                    df.drop(['tournament', 'quantity'], axis=1, inplace=True)
                    drafted_dfs.append(df)

            for matches_csv in matches_csvs:
                with z.open(matches_csv) as f:
                    df = pd.read_csv(f)
                    df['draft_id'] = date_str  # add draft_id column here
                    df['season_id'] = get_season_for_date(date_str, season_config)
                    df.drop('tournamentDate', axis=1, inplace=True)
                    matches_dfs.append(df)

    if not drafted_dfs:
        raise FileNotFoundError("No drafted deck CSVs found in ZIPs.")
    if not matches_dfs:
        raise FileNotFoundError("No matches CSVs found in ZIPs.")

    drafted_df = pd.concat(drafted_dfs, ignore_index=True)
    matches_df = pd.concat(matches_dfs, ignore_index=True)

    return drafted_df, matches_df


def save_dataframes_to_csv(drafted_df, matches_df, output_dir=Path("data/processed")):
    """
    Saves the drafted and match DataFrames to CSV files in output_dir.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    drafted_path = output_dir / "drafted_decks.csv"
    matches_path = output_dir / "matches.csv"

    drafted_df.to_csv(drafted_path, index=False)
    matches_df.to_csv(matches_path, index=False)

    print(f"Saved drafted decks to {drafted_path}")
    print(f"Saved matches to {matches_path}")

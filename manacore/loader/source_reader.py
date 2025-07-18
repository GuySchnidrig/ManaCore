import os
import zipfile
import pandas as pd
from pathlib import Path
import re
from manacore.config.get_seasons import load_season_config, get_season_for_date

def extract_date_from_filename(filename: str) -> str:
    """
    Extract a date string in YYYYMMDD format from a filename containing a date in YYYY_MM_DD format.

    Parameters:
    -----------
    filename : str
        The filename from which to extract the date (e.g., "cube_snapshot_2024_07_18.json").

    Returns:
    --------
    str
        The extracted date in 'YYYYMMDD' format. Returns 'unknown_date' if no valid date is found.
    """
    # Extract date in YYYY_MM_DD format from filename, return YYYYMMDD string
    match = re.search(r'(\d{4})_(\d{2})_(\d{2})', filename)
    if match:
        return f"{match.group(1)}{match.group(2)}{match.group(3)}"
    return "unknown_date"
    
def read_all_csvs_from_zips():
    """
    Read and combine drafted deck and match CSV files from ZIP archives in the raw data directory.

    Scans the "data/raw" directory for `.zip` files, extracts relevant CSVs,
    annotates them with draft and season metadata, and returns two combined DataFrames.

    Returns:
    --------
    tuple (pd.DataFrame, pd.DataFrame)
        - drafted_df: Combined DataFrame of all drafted deck entries with 'draft_id' and 'season_id'.
        - matches_df: Combined DataFrame of all match entries with 'draft_id' and 'season_id'.

    Raises:
    -------
    FileNotFoundError
        If no drafted deck CSVs or no match CSVs are found in the ZIP files.

    Behavior:
    ---------
    - Extracts `drafted_decks` and `matches` CSV files from each ZIP archive.
    - Infers `draft_id` from the ZIP filename using the format YYYY_MM_DD.
    - Determines the `season_id` using the extracted date and season config.
    - Drops unnecessary columns (`'tournament'`, `'quantity'`, and `'tournamentDate'`).
    - Sorts the resulting DataFrames by `draft_id` while preserving original internal order.
    """
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

    # Sort both DataFrames by draft_id while keeping the internal order stable
    drafted_df = drafted_df.sort_values(by='draft_id', kind='stable')
    matches_df = matches_df.sort_values(by='draft_id', kind='stable')

    return drafted_df, matches_df


def save_dataframes_to_csv(drafted_df, matches_df, output_dir=Path("data/processed")):
    """
    Save drafted and match DataFrames to CSV files in a specified output directory.

    Parameters:
    -----------
    drafted_df : pd.DataFrame
        DataFrame containing drafted deck data with columns including:
        ['season_id', 'draft_id', 'player', 'archetype', 'decktype', 'scryfallId'].

    matches_df : pd.DataFrame
        DataFrame containing match results with columns including:
        ['season_id', 'draft_id', 'player1', 'player2', 'player1Wins', 'player2Wins', 'draws', 'round'].

    output_dir : pathlib.Path, optional
        Path to the output directory where CSV files will be saved (default: "data/processed").

    Behavior:
    ---------
    - Reorders columns of both DataFrames to a standard format.
    - Ensures the output directory exists.
    - Saves `drafted_df` to 'drafted_decks.csv' and `matches_df` to 'matches.csv' within the output directory.
    - Prints confirmation messages with output paths.
    """
    
    drafted_col_order = ['season_id', 'draft_id', 'player', 'archetype', 'decktype', 'scryfallId'] 
    drafted_df = drafted_df[drafted_col_order]
    
    matches_col_order = ['season_id', 'draft_id','player1','player2','player1Wins','player2Wins','draws','round'] 
    matches_df = matches_df[matches_col_order]
    
    output_dir.mkdir(parents=True, exist_ok=True)

    drafted_path = output_dir / "drafted_decks.csv"
    matches_path = output_dir / "matches.csv"

    drafted_df.to_csv(drafted_path, index=False)
    matches_df.to_csv(matches_path, index=False)

    print(f"Saved drafted decks to {drafted_path}")
    print(f"Saved matches to {matches_path}")

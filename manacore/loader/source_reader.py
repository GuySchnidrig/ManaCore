import os
import re
import zipfile
import pandas as pd
from pathlib import Path
from typing import List, Tuple

from manacore.config.get_seasons import load_season_config, get_season_for_date


def extract_date_from_filename(filename: str) -> str:
    """Extract YYYYMMDD from a filename with format like '2024_07_18'."""
    match = re.search(r'(\d{4})_(\d{2})_(\d{2})', filename)
    return f"{match.group(1)}{match.group(2)}{match.group(3)}" if match else "unknown_date"


def get_zip_file_paths(base_path: str = "data/raw") -> List[str]:
    """Return all .zip file paths in the given directory."""
    return [
        os.path.join(base_path, f)
        for f in os.listdir(base_path)
        if f.endswith('.zip')
    ]


def read_csv_from_zip(zip_file: zipfile.ZipFile, file_name: str) -> pd.DataFrame:
    """Read a CSV file from within a ZIP archive."""
    with zip_file.open(file_name) as f:
        return pd.read_csv(f)


def process_drafted_csv(df: pd.DataFrame, draft_id: str, season_id: str) -> pd.DataFrame:
    """Annotate and clean a drafted_decks DataFrame."""
    df = df.copy()
    df["draft_id"] = draft_id
    df["season_id"] = season_id
    return df.drop(columns=["tournament", "quantity"], errors="ignore")


def process_matches_csv(df: pd.DataFrame, draft_id: str, season_id: str) -> pd.DataFrame:
    """Annotate and clean a matches DataFrame."""
    df = df.copy()
    df["draft_id"] = draft_id
    df["season_id"] = season_id
    return df.drop(columns=["tournamentDate"], errors="ignore")


def read_all_csvs_from_zips() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Scan ZIP files in the raw data folder, extract and annotate drafted_decks and matches CSVs,
    and return them as two combined DataFrames.
    """
    zip_files = get_zip_file_paths()
    if not zip_files:
        raise FileNotFoundError("No ZIP files found in the raw data folder.")

    drafted_dfs, matches_dfs = [], []
    season_config = load_season_config()

    for zip_path in zip_files:
        date_str = extract_date_from_filename(os.path.basename(zip_path))
        season_id = get_season_for_date(date_str, season_config)

        with zipfile.ZipFile(zip_path, 'r') as z:
            for name in z.namelist():
                name_lower = name.lower()
                if "drafted_decks" in name_lower:
                    df = read_csv_from_zip(z, name)
                    drafted_dfs.append(process_drafted_csv(df, date_str, season_id))
                elif "matches" in name_lower:
                    df = read_csv_from_zip(z, name)
                    matches_dfs.append(process_matches_csv(df, date_str, season_id))

    if not drafted_dfs:
        raise FileNotFoundError("No drafted deck CSVs found in ZIPs.")
    if not matches_dfs:
        raise FileNotFoundError("No matches CSVs found in ZIPs.")

    drafted_df = pd.concat(drafted_dfs, ignore_index=True).sort_values(by="draft_id", kind="stable")
    matches_df = pd.concat(matches_dfs, ignore_index=True).sort_values(by="draft_id", kind="stable")

    return drafted_df, matches_df


def save_dataframes_to_csv(drafted_df: pd.DataFrame, matches_df: pd.DataFrame, output_dir: Path = Path("data/processed")) -> None:
    """
    Save drafted and match DataFrames to CSV files in the specified output directory.
    Also exports a drafts.csv with draft_id and timestamp (YYYY-MM-DD).
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    drafted_out = drafted_df[["season_id", "draft_id", "player", "archetype", "decktype", "scryfallId"]]
    
    matches_df['match_id'] = (
    matches_df['season_id'].astype(str) + '_' +
    matches_df['draft_id'].astype(str) + '_' +
    matches_df['round'].astype(str) + '_' +
    matches_df['player1'].astype(str) + '_' +
    matches_df['player2'].astype(str))

    matches_out = matches_df[["season_id", "draft_id","match_id", "player1", "player2", "player1Wins", "player2Wins", "draws", "round"]]

    drafted_path = output_dir / "drafted_decks.csv"
    matches_path = output_dir / "matches.csv"
    drafts_path = output_dir / "drafts.csv"

    # Save drafted decks and matches
    drafted_out.to_csv(drafted_path, index=False)
    matches_out.to_csv(matches_path, index=False)

    # Create drafts dataframe with timestamp column (YYYY-MM-DD format)
    drafts_df = drafted_out[["season_id", "draft_id"]].drop_duplicates()
    drafts_df['timestamp'] = pd.to_datetime(drafts_df['draft_id'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
    drafts_df = drafts_df.sort_values(by=["season_id", "draft_id"])

    # Save drafts.csv with timestamp
    drafts_df.to_csv(drafts_path, index=False)

    print(f"Saved drafted decks to {drafted_path}")
    print(f"Saved matches to {matches_path}")
    print(f"Saved drafts list to {drafts_path}")

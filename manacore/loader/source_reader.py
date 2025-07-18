import os
import zipfile
import pandas as pd
from pathlib import Path
import re
import logging
from typing import Tuple, List, Optional
from datetime import datetime
from manacore.config.get_seasons import load_season_config, get_season_for_date

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataProcessingError(Exception):
    """Custom exception for data processing errors."""
    pass

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
    
    Raises:
    -------
    ValueError
        If the extracted date is invalid (e.g., month > 12, day > 31).
    """
    # Extract date in YYYY_MM_DD format from filename, return YYYYMMDD string
    match = re.search(r'(\d{4})_(\d{2})_(\d{2})', filename)
    if not match:
        logger.warning(f"No date pattern found in filename: {filename}")
        return "unknown_date"
    
    year, month, day = match.groups()
    
    # Validate date components
    try:
        datetime(int(year), int(month), int(day))
    except ValueError as e:
        logger.error(f"Invalid date extracted from {filename}: {year}-{month}-{day}")
        raise ValueError(f"Invalid date in filename {filename}: {e}")
    
    return f"{year}{month}{day}"

def get_zip_files(base_path: str) -> List[str]:
    """
    Get list of ZIP files from the specified directory.
    
    Parameters:
    -----------
    base_path : str
        Directory path to search for ZIP files.
        
    Returns:
    --------
    List[str]
        List of full paths to ZIP files.
        
    Raises:
    -------
    FileNotFoundError
        If the base directory doesn't exist.
    DataProcessingError
        If no ZIP files are found.
    """
    if not os.path.exists(base_path):
        raise FileNotFoundError(f"Base directory not found: {base_path}")
    
    zip_files = [os.path.join(base_path, f) for f in os.listdir(base_path) if f.endswith('.zip')]
    
    if not zip_files:
        raise DataProcessingError(f"No ZIP files found in directory: {base_path}")
    
    logger.info(f"Found {len(zip_files)} ZIP files to process")
    return zip_files

def process_csv_from_zip(zip_file: zipfile.ZipFile, csv_name: str, date_str: str, 
                        season_id: str, csv_type: str) -> pd.DataFrame:
    """
    Process a single CSV file from a ZIP archive.
    
    Parameters:
    -----------
    zip_file : zipfile.ZipFile
        Open ZIP file object.
    csv_name : str
        Name of the CSV file within the ZIP.
    date_str : str
        Date string for the draft_id.
    season_id : str
        Season identifier.
    csv_type : str
        Type of CSV ('drafted_decks' or 'matches').
        
    Returns:
    --------
    pd.DataFrame
        Processed DataFrame with added metadata columns.
    """
    try:
        with zip_file.open(csv_name) as f:
            df = pd.read_csv(f)
            
        # Add metadata columns
        df['draft_id'] = date_str
        df['season_id'] = season_id
        
        # Drop unnecessary columns based on CSV type
        if csv_type == 'drafted_decks':
            columns_to_drop = ['tournament', 'quantity']
        elif csv_type == 'matches':
            columns_to_drop = ['tournamentDate']
        else:
            columns_to_drop = []
            
        # Only drop columns that exist in the DataFrame
        existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
        if existing_columns_to_drop:
            df.drop(existing_columns_to_drop, axis=1, inplace=True)
            
        logger.debug(f"Processed {csv_name}: {len(df)} rows")
        return df
        
    except Exception as e:
        logger.error(f"Error processing {csv_name}: {e}")
        raise DataProcessingError(f"Failed to process {csv_name}: {e}")

def read_all_csvs_from_zips(base_path: str = "data/raw") -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Read and combine drafted deck and match CSV files from ZIP archives in the raw data directory.

    Parameters:
    -----------
    base_path : str, optional
        Path to the directory containing ZIP files (default: "data/raw").

    Returns:
    --------
    Tuple[pd.DataFrame, pd.DataFrame]
        - drafted_df: Combined DataFrame of all drafted deck entries with 'draft_id' and 'season_id'.
        - matches_df: Combined DataFrame of all match entries with 'draft_id' and 'season_id'.

    Raises:
    -------
    FileNotFoundError
        If the base directory doesn't exist.
    DataProcessingError
        If no ZIP files are found or if no relevant CSVs are found.
    ValueError
        If invalid dates are found in filenames.

    Behavior:
    ---------
    - Extracts `drafted_decks` and `matches` CSV files from each ZIP archive.
    - Infers `draft_id` from the ZIP filename using the format YYYY_MM_DD.
    - Determines the `season_id` using the extracted date and season config.
    - Drops unnecessary columns (`'tournament'`, `'quantity'`, and `'tournamentDate'`).
    - Sorts the resulting DataFrames by `draft_id` while preserving original internal order.
    """
    logger.info(f"Starting CSV processing from directory: {base_path}")
    
    zip_files = get_zip_files(base_path)
    drafted_dfs = []
    matches_dfs = []
    
    try:
        season_config = load_season_config()
    except Exception as e:
        logger.error(f"Failed to load season config: {e}")
        raise DataProcessingError(f"Failed to load season config: {e}")

    for zip_file_path in zip_files:
        logger.info(f"Processing ZIP file: {os.path.basename(zip_file_path)}")
        
        try:
            date_str = extract_date_from_filename(os.path.basename(zip_file_path))
            
            if date_str == "unknown_date":
                logger.warning(f"Skipping file with unknown date: {zip_file_path}")
                continue
                
            season_id = get_season_for_date(date_str, season_config)
            
            with zipfile.ZipFile(zip_file_path, 'r') as z:
                # Find CSV files
                drafted_decks_csvs = [name for name in z.namelist() 
                                    if 'drafted_decks' in name.lower() and name.endswith('.csv')]
                matches_csvs = [name for name in z.namelist() 
                               if 'matches' in name.lower() and name.endswith('.csv')]

                # Process drafted decks CSVs
                for drafted_csv in drafted_decks_csvs:
                    df = process_csv_from_zip(z, drafted_csv, date_str, season_id, 'drafted_decks')
                    drafted_dfs.append(df)

                # Process matches CSVs
                for matches_csv in matches_csvs:
                    df = process_csv_from_zip(z, matches_csv, date_str, season_id, 'matches')
                    matches_dfs.append(df)
                    
        except Exception as e:
            logger.error(f"Error processing {zip_file_path}: {e}")
            raise DataProcessingError(f"Error processing {zip_file_path}: {e}")

    # Validate that we found data
    if not drafted_dfs:
        raise DataProcessingError("No drafted deck CSVs found in any ZIP files.")
    if not matches_dfs:
        raise DataProcessingError("No matches CSVs found in any ZIP files.")

    logger.info(f"Combining {len(drafted_dfs)} drafted deck DataFrames and {len(matches_dfs)} match DataFrames")

    # Combine DataFrames
    try:
        drafted_df = pd.concat(drafted_dfs, ignore_index=True)
        matches_df = pd.concat(matches_dfs, ignore_index=True)
    except Exception as e:
        logger.error(f"Error combining DataFrames: {e}")
        raise DataProcessingError(f"Error combining DataFrames: {e}")

    # Sort both DataFrames by draft_id while keeping the internal order stable
    drafted_df = drafted_df.sort_values(by='draft_id', kind='stable')
    matches_df = matches_df.sort_values(by='draft_id', kind='stable')

    logger.info(f"Successfully processed {len(drafted_df)} drafted deck rows and {len(matches_df)} match rows")
    
    return drafted_df, matches_df

def validate_dataframes(drafted_df: pd.DataFrame, matches_df: pd.DataFrame) -> None:
    """
    Validate the structure and content of the processed DataFrames.
    
    Parameters:
    -----------
    drafted_df : pd.DataFrame
        DataFrame containing drafted deck data.
    matches_df : pd.DataFrame
        DataFrame containing match results.
        
    Raises:
    -------
    DataProcessingError
        If validation fails.
    """
    # Check for required columns
    required_drafted_cols = ['season_id', 'draft_id', 'player', 'archetype', 'decktype', 'scryfallId']
    required_matches_cols = ['season_id', 'draft_id', 'player1', 'player2', 'player1Wins', 'player2Wins', 'draws', 'round']
    
    missing_drafted_cols = [col for col in required_drafted_cols if col not in drafted_df.columns]
    missing_matches_cols = [col for col in required_matches_cols if col not in matches_df.columns]
    
    if missing_drafted_cols:
        raise DataProcessingError(f"Missing required columns in drafted_df: {missing_drafted_cols}")
    if missing_matches_cols:
        raise DataProcessingError(f"Missing required columns in matches_df: {missing_matches_cols}")
    
    # Check for empty DataFrames
    if drafted_df.empty:
        raise DataProcessingError("Drafted DataFrame is empty")
    if matches_df.empty:
        raise DataProcessingError("Matches DataFrame is empty")
    
    logger.info("DataFrame validation passed")

def save_dataframes_to_csv(drafted_df: pd.DataFrame, matches_df: pd.DataFrame, 
                          output_dir: Path = Path("data/processed")) -> None:
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

    Raises:
    -------
    DataProcessingError
        If saving fails or if required columns are missing.

    Behavior:
    ---------
    - Validates DataFrames before saving.
    - Reorders columns of both DataFrames to a standard format.
    - Ensures the output directory exists.
    - Saves `drafted_df` to 'drafted_decks.csv' and `matches_df` to 'matches.csv' within the output directory.
    - Logs confirmation messages with output paths.
    """
    logger.info(f"Saving DataFrames to directory: {output_dir}")
    
    # Validate DataFrames
    validate_dataframes(drafted_df, matches_df)
    
    # Define column orders
    drafted_col_order = ['season_id', 'draft_id', 'player', 'archetype', 'decktype', 'scryfallId'] 
    matches_col_order = ['season_id', 'draft_id', 'player1', 'player2', 'player1Wins', 'player2Wins', 'draws', 'round'] 
    
    # Reorder columns
    drafted_df_ordered = drafted_df[drafted_col_order]
    matches_df_ordered = matches_df[matches_col_order]
    
    # Create output directory
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise DataProcessingError(f"Failed to create output directory {output_dir}: {e}")

    # Define output paths
    drafted_path = output_dir / "drafted_decks.csv"
    matches_path = output_dir / "matches.csv"

    # Save DataFrames
    try:
        drafted_df_ordered.to_csv(drafted_path, index=False)
        matches_df_ordered.to_csv(matches_path, index=False)
    except Exception as e:
        raise DataProcessingError(f"Failed to save CSV files: {e}")

    logger.info(f"Saved drafted decks to {drafted_path} ({len(drafted_df_ordered)} rows)")
    logger.info(f"Saved matches to {matches_path} ({len(matches_df_ordered)} rows)")

def main(input_dir: str = "data/raw", output_dir: str = "data/processed") -> None:
    """
    Main function to process all CSV files from ZIP archives.
    
    Parameters:
    -----------
    input_dir : str, optional
        Directory containing ZIP files (default: "data/raw").
    output_dir : str, optional
        Directory to save processed CSV files (default: "data/processed").
    """
    try:
        drafted_df, matches_df = read_all_csvs_from_zips(input_dir)
        save_dataframes_to_csv(drafted_df, matches_df, Path(output_dir))
        logger.info("Data processing completed successfully")
    except Exception as e:
        logger.error(f"Data processing failed: {e}")
        raise

if __name__ == "__main__":
    main()
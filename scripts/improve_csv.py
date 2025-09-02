import os
import json
import gzip
import pandas as pd
from pathlib import Path

def process_csvs_with_scryfall_names(csv_directory, json_file_path="data/cards/scryfall_filtered_cards.json.gz"):
    """
    Reads all CSV files in a directory, checks for scryfallId column,
    and adds a card_name column based on the Scryfall JSON data.
    
    Args:
        csv_directory (str): Path to directory containing CSV files
        json_file_path (str): Path to the gzipped JSON file with card data
    
    Returns:
        dict: Dictionary with CSV filenames as keys and processing status as values
    """
    
    # Load the Scryfall card data
    print(f"Loading card data from {json_file_path}...")
    try:
        with gzip.open(json_file_path, 'rt', encoding='utf-8') as f:
            card_data = json.load(f)
        
        # Create a lookup dictionary: scryfallId -> card name
        scryfall_lookup = {}
        if isinstance(card_data, list):
            # If it's a list of cards
            for card in card_data:
                if 'id' in card and 'name' in card:
                    scryfall_lookup[card['id']] = card['name']
        elif isinstance(card_data, dict):
            # If it's already a dictionary format
            scryfall_lookup = {k: v.get('name', '') for k, v in card_data.items() if 'name' in v}
        
        print(f"Loaded {len(scryfall_lookup)} card mappings")
        
    except Exception as e:
        print(f"Error loading card data: {e}")
        return {}
    
    # Process CSV files
    csv_directory = Path(csv_directory)
    results = {}
    
    if not csv_directory.exists():
        print(f"Directory {csv_directory} does not exist")
        return {}
    
    csv_files = list(csv_directory.glob("*.csv"))
    print(f"Found {len(csv_files)} CSV files to process")
    
    for csv_file in csv_files:
        print(f"\nProcessing {csv_file.name}...")
        
        try:
            # Read the CSV file
            df = pd.read_csv(csv_file)
            
            # Check if scryfallId column exists (case-insensitive)
            scryfall_col = None
            for col in df.columns:
                if col.lower().strip() in ['scryfallid', 'scryfall_id', 'id']:
                    scryfall_col = col
                    break
            
            if scryfall_col is None:
                results[csv_file.name] = "No scryfallId column found"
                print(f"  ❌ No scryfallId column found in {csv_file.name}")
                continue
            
            # Add card_name column if it doesn't exist
            if 'card_name' not in df.columns:
                # Map scryfallId to card names
                card_names = df[scryfall_col].map(scryfall_lookup)
                
                # Find the position of the scryfallId column
                scryfall_col_idx = df.columns.get_loc(scryfall_col)
                
                # Insert card_name column right after scryfallId
                df.insert(scryfall_col_idx + 1, 'card_name', card_names)
                
                # Count successful mappings
                mapped_count = df['card_name'].notna().sum()
                total_count = len(df)
                
                print(f"  ✅ Added card_name column next to {scryfall_col}. Mapped {mapped_count}/{total_count} cards")
                
                # Save the updated CSV
                df.to_csv(csv_file, index=False)
                results[csv_file.name] = f"Success: {mapped_count}/{total_count} cards mapped"
                
            else:
                print(f"  ⚠️  card_name column already exists in {csv_file.name}")
                results[csv_file.name] = "card_name column already exists"
                
        except Exception as e:
            error_msg = f"Error processing file: {e}"
            print(f"  ❌ {error_msg}")
            results[csv_file.name] = error_msg
    
    return results

def add_player_ids_to_csvs(csv_directory="data/processed", players_csv_path="data/processed/players.csv"):
    """
    Reads all CSVs (except players.csv) and adds a player_id column next to the 'player' column.
    Only considers columns literally named 'player'. Ensures player_id is integer.
    """
    csv_directory = Path(csv_directory)
    results = {}

    if not csv_directory.exists():
        print(f"Directory {csv_directory} does not exist")
        return {}

    # Load players.csv mapping
    players_df = pd.read_csv(players_csv_path)
    player_lookup = dict(zip(players_df['player'], players_df['player_id']))

    csv_files = list(csv_directory.glob("*.csv"))

    for csv_file in csv_files:
        if csv_file.name == "players.csv":
            continue  # skip players.csv

        print(f"\nProcessing {csv_file.name}...")
        try:
            df = pd.read_csv(csv_file)

            # Only consider column literally named 'player'
            if 'player' not in df.columns:
                results[csv_file.name] = "No 'player' column found"
                continue

            new_col = 'player_id'
            if new_col not in df.columns:
                col_idx = df.columns.get_loc('player')
                # Map player names to IDs and convert to nullable integer
                df.insert(col_idx + 1, new_col, df['player'].map(player_lookup).astype("Int64"))
                df.to_csv(csv_file, index=False)
                results[csv_file.name] = "Success: Added 'player_id' column"
                print(f"  ✅ Added 'player_id' column next to 'player'")
            else:
                results[csv_file.name] = "'player_id' column already exists"
                print(f"  ⚠️  'player_id' column already exists")

        except Exception as e:
            results[csv_file.name] = f"Error processing file: {e}"
            print(f"  ❌ Error: {e}")

    return results


def main():
    """
    Command-line interface for the CSV processor
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="Add card names to CSV files based on Scryfall IDs")
    parser.add_argument("--json-file", "-j", default="data/cards/scryfall_filtered_cards.json.gz",
                       help="Path to Scryfall JSON file (default: data/cards/scryfall_filtered_cards.json.gz)")
    
    args = parser.parse_args()
    
    # Process the files in data/processed directory
    csv_dir = "data/processed"
    card_results  = process_csvs_with_scryfall_names(csv_dir, args.json_file)
    
    # Step 2: Add player IDs
    player_results = add_player_ids_to_csvs(csv_dir)
    
    # Print summary
    print("\n" + "="*50)
    print("PROCESSING SUMMARY")
    print("="*50)
    for filename, status in card_results .items():
        print(f"{filename}: {status}")
        
    for filename, status in player_results.items():
        print(f"{filename}: {status}")
        
    # Exit with error code if any files failed
        failed_files = [f for f, s in {**card_results, **player_results}.items() if s.startswith("Error")]
        
        if failed_files:
            print(f"\n❌ {len(failed_files)} files failed to process")
            exit(1)
        else:
            print(f"\n✅ Successfully processed {len(card_results) + len(player_results)} files")


if __name__ == "__main__":
    main()
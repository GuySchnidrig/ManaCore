from manacore.loader.source_reader import read_all_csvs_from_zips, save_dataframes_to_csv

def main():
    drafted_df, matches_df = read_all_csvs_from_zips()
    save_dataframes_to_csv(drafted_df, matches_df)

if __name__ == "__main__":
    main()

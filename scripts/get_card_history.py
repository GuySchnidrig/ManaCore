from manacore.cardbase import get_cardbase

def main():
    posts_with_season = get_cardbase.load_combined_history_data('data/processed/history')
    history_records = get_cardbase.process_history_entries(posts_with_season)

    get_cardbase.export_history_csv(history_records)

if __name__ == "__main__":
    main()


from  manacore.standings.standings_calculator import calculate_standings, save_standings_to_csv

def main():
    standings_df = calculate_standings()
    save_standings_to_csv(standings_df)

if __name__ == "__main__":
    main()

from manacore.elo.elo_calculator import process_matches


def main():
    process_matches("data/processed/matches.csv", "data/processed/elo_development.csv")

if __name__ == "__main__":
    main()


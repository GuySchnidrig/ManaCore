
from  manacore.standings.standings_calculator import *

def main():
    standings_df = calculate_standings()
    player_ids_df = extract_player_ids(standings_df)
    save_standings_to_csv(standings_df)
    save_players_id_to_csv(player_ids_df)

if __name__ == "__main__":
    main()

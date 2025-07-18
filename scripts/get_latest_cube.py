from manacore.cardbase import get_cardbase
from manacore.config.get_seasons import load_season_config, load_season_start_dates

def main():
    cube_seasons = load_season_config()
    season_start_dates = load_season_start_dates()
    mainboard_data = get_cardbase.fetch_cube_mainboard(cube_seasons, season_start_dates)

    get_cardbase.export_mainboard_csv(mainboard_data)
if __name__ == "__main__":
    main()

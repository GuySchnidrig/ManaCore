from manacore.deckcolor.set_deckcolor import (
    load_data,
    prepare_cube,
    resolve_tags,
    assign_deck_ids,
    assign_deck_colours,
    remove_lands_from_deck_colours
)
import pandas as pd
from manacore.deckcolor.set_deckcolor import load_scryfall_data, expand_multicolor_tags  # your new functions

# -----------------------
# Main Callable Function
# -----------------------
def process_drafted_decks(
    scryfall_path: str = "data/cards/scryfall_filtered_cards.json.gz",
    save_path: str = "data/processed/drafted_decks.csv"
) -> pd.DataFrame:
    """
    Load, process, and save drafted decks with tags and deck colors.
    Uses local filtered Scryfall JSON to expand multicolor tags.
    """
    # Load Scryfall filtered cards
    card_index = load_scryfall_data(scryfall_path)

    # Load decks and cube data
    decks, cube_main, cube_history = load_data()
    main_tags, history_tags = prepare_cube(cube_main, cube_history)

    # Resolve tags per card
    decks["tags"] = decks["scryfallId"].apply(lambda sid: resolve_tags(sid, main_tags, history_tags))

    # Assign unique deck IDs
    decks = assign_deck_ids(decks)

    # Expand multicolor tags using local Scryfall data
    decks["tags"] = decks.apply(lambda row: expand_multicolor_tags(row["tags"], row["scryfallId"], card_index), axis=1)

    # Assign main deck colors
    deck_colours_df = assign_deck_colours(decks)
    decks = decks.merge(deck_colours_df, on="deck_id", how="left")

    # Remove irrelevant color tags
    decks = remove_lands_from_deck_colours(decks)

    # Save processed decks
    decks.to_csv(save_path, index=False)
    print(f"drafted_decks saved successfully to {save_path}!")
    return decks


# -----------------------
# Main Execution
# -----------------------
if __name__ == "__main__":
    process_drafted_decks()

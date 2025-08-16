from manacore.deckcolor.set_deckcolor import (
    load_data,
    prepare_cube,
    resolve_tags,
    assign_deck_ids,
    expand_multicolor_tags,
    assign_deck_colours,
    remove_lands_from_deck_colours
)
import pandas as pd


# -----------------------
# Main Callable Function
# -----------------------
def process_drafted_decks(save_path: str = "data/processed/drafted_decks.csv") -> pd.DataFrame:
    """Load, process, and save drafted decks with tags and deck colors."""
    decks, cube_main, cube_history = load_data()
    main_tags, history_tags = prepare_cube(cube_main, cube_history)

    decks["tags"] = decks["scryfallId"].apply(lambda sid: resolve_tags(sid, main_tags, history_tags))
    decks = assign_deck_ids(decks)
    decks["tags"] = decks.apply(lambda row: expand_multicolor_tags(row["tags"], row["scryfallId"]), axis=1)

    deck_colours_df = assign_deck_colours(decks)
    decks = decks.merge(deck_colours_df, on="deck_id", how="left")
    decks = remove_lands_from_deck_colours(decks)

    decks.to_csv(save_path, index=False)
    print(f"drafted_decks saved successfully to {save_path}!")
    return decks


# -----------------------
# Main Execution
# -----------------------
if __name__ == "__main__":
    process_drafted_decks()
import os
from collections import Counter
from typing import List, Tuple, Dict
import pandas as pd
import gzip
import json
import ast

from manacore.statistics.statistics_calculator import load_drafted_decks


# -----------------------
# Data Loading Functions
# -----------------------
def load_data() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    decks = load_drafted_decks(base_path="data/processed")
    cube_main = pd.read_csv("data/processed/cube_mainboard.csv")
    cube_history = pd.read_csv("data/processed/cube_history.csv")
    return decks, cube_main, cube_history


def parse_tags(val):
    """Ensure CSV-stored tags are converted to lists."""
    if pd.isna(val) or val in ("", "[]"):
        return []
    try:
        return ast.literal_eval(val) if isinstance(val, str) else val
    except Exception:
        return []


def prepare_cube(cube_main: pd.DataFrame, cube_history: pd.DataFrame):
    """Return dictionaries mapping scryfallId -> tags."""
    for df in (cube_main, cube_history):
        df["tags"] = df["tags"].apply(parse_tags)

    main_tags = cube_main.groupby("scryfallId")["tags"].first().to_dict()
    history_tags = cube_history.groupby("scryfallId")["tags"].first().to_dict()
    return main_tags, history_tags


# -----------------------
# Deck Tag Processing
# -----------------------
def resolve_tags(sid: str, main_tags: Dict[str, List[str]], history_tags: Dict[str, List[str]]) -> List[str]:
    return main_tags.get(sid, []) or history_tags.get(sid, [])


def assign_deck_ids(decks: pd.DataFrame) -> pd.DataFrame:
    """Assign a unique deck_id per (season_id, draft_id, player)."""
    decks["deck_id"] = decks.groupby(["season_id", "draft_id", "player"]).ngroup() + 1

    cols = decks.columns.tolist()
    draft_idx = cols.index("draft_id")
    cols.insert(draft_idx + 1, cols.pop(cols.index("deck_id")))
    return decks[cols]


# -----------------------
# Deck Color Processing
# -----------------------
def assign_deck_colours(decks: pd.DataFrame) -> pd.DataFrame:
    """Assign main deck colors based on tag counts, ignoring 'Lands'."""
    deck_colours = []

    for deck_id, group in decks.groupby("deck_id"):
        all_tags = [tag for tags_list in group["tags"] for tag in tags_list if tag.lower() != "lands"]
        tag_counts = Counter(all_tags)
        main_colours = [tag for tag, count in tag_counts.items() if count >= 4]

        deck_colours.append({
            "deck_id": deck_id,
            "deck_colour": main_colours
        })

    return pd.DataFrame(deck_colours)


def load_scryfall_data(local_path: str) -> Dict[str, Dict]:
    """
    Load Scryfall filtered bulk data from a local JSON gzip file.
    Returns a dictionary mapping card ID to the card data.
    """
    with gzip.open(local_path, "rt", encoding="utf-8") as f:
        data = json.load(f)
    return {card["id"]: card for card in data}


def get_scryfall_colors_local(scryfall_id: str, card_index: Dict[str, Dict]) -> List[str]:
    """
    Get colors of a card using the local filtered Scryfall index.
    Returns a list of color codes, e.g., ['W', 'U'], or empty list if not found.
    """
    card = card_index.get(scryfall_id)
    if card:
        return card.get("colors", [])
    return []

# -----------------------
# Expand multicolor tags
# -----------------------
def expand_multicolor_tags(tags: List[str], scryfall_id: str, card_index: Dict[str, Dict]) -> List[str]:
    """
    Replace 'Multicolor'/'Multicolored' tags with actual colors from local Scryfall data.
    Fill empty tags with colors from Scryfall if available.
    """
    color_map = {"W": "White", "U": "Blue", "B": "Black", "R": "Red", "G": "Green"}
    scryfall_colors = get_scryfall_colors_local(scryfall_id, card_index)

    if not tags:
        # If no tags, use colors from Scryfall
        return [color_map[c] for c in scryfall_colors if c in color_map]

    new_tags = []
    for t in tags:
        if t.lower() in ("multicolor", "multicolored"):
            new_tags.extend([color_map[c] for c in scryfall_colors if c in color_map])
        else:
            new_tags.append(t)

    return new_tags


def expand_multicolor_tags_in_decks(decks: pd.DataFrame, card_index: Dict[str, Dict]) -> pd.DataFrame:
    """
    Expand multicolor tags for all cards in the deck using local Scryfall data.
    Updates the 'tags' column in-place.
    """
    decks["tags"] = decks.apply(
        lambda row: expand_multicolor_tags(row["tags"], row["scryfallId"], card_index),
        axis=1
    )
    return decks


def remove_lands_from_deck_colours(decks: pd.DataFrame) -> pd.DataFrame:
    """Remove 'Land', 'Lands', 'Colorless', or 'Multicolored' from deck_colour lists."""
    if "deck_colour" in decks.columns:
        decks["deck_colour"] = decks["deck_colour"].apply(
            lambda colours: [c for c in colours if c.lower() not in ("land", "lands", "colorless", "multicolored")]
        )
    return decks

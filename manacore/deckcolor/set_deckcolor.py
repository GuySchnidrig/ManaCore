import os
from collections import Counter
from typing import List, Tuple

import pandas as pd
import requests
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
        return eval(val) if isinstance(val, str) else val
    except Exception:
        return []


def prepare_cube(cube_main: pd.DataFrame, cube_history: pd.DataFrame):
    for df in (cube_main, cube_history):
        df["tags"] = df["tags"].apply(parse_tags)

    main_tags = cube_main.groupby("scryfallId")["tags"].first().to_dict()
    history_tags = cube_history.groupby("scryfallId")["tags"].first().to_dict()
    return main_tags, history_tags


# -----------------------
# Deck Tag Processing
# -----------------------
def resolve_tags(sid, main_tags, history_tags):
    tags = main_tags.get(sid, []) or history_tags.get(sid, [])
    return tags


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


def get_scryfall_colors(scryfall_id: str) -> List[str]:
    """Fetch card colors from Scryfall by Scryfall ID."""
    url = f"https://api.scryfall.com/cards/{scryfall_id}"
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        return resp.json().get("colors", [])
    except requests.RequestException:
        print(f"Failed to fetch Scryfall data for {scryfall_id}")
        return []


def expand_multicolor_tags(tags: List[str], scryfall_id: str) -> List[str]:
    """Replace 'Multicolor' tag with actual Scryfall colors."""
    if "Multicolor" in tags:
        color_map = {"W": "White", "U": "Blue", "B": "Black", "R": "Red", "G": "Green"}
        return [color_map[c] for c in get_scryfall_colors(scryfall_id) if c in color_map]
    return tags


def remove_lands_from_deck_colours(decks: pd.DataFrame) -> pd.DataFrame:
    """Remove 'Land', 'Lands', 'Colorless', or 'Multicolored' from deck_colour lists."""
    if "deck_colour" in decks.columns:
        decks["deck_colour"] = decks["deck_colour"].apply(
            lambda colours: [c for c in colours if c.lower() not in ("land", "lands", "colorless", "multicolored")]
        )
    return decks



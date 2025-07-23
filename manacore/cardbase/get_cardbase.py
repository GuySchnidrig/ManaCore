import os
import json
import requests
import pandas as pd
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Optional


def ensure_dir(path: str):
    """Ensure directory exists for a given file or folder path."""
    os.makedirs(os.path.dirname(path) if os.path.splitext(path)[1] else path, exist_ok=True)


def fetch_cube_mainboard(season_data: Dict, season_start_dates: Dict) -> List[Dict]:
    """
    Fetch mainboard card data for each season's cube from CubeCobra.
    """
    all_cards = []

    for season_id, meta in season_data.items():
        cube_id = meta["cube_id"]
        url = f"https://cubecobra.com/cube/api/cubeJSON/{cube_id}"
        resp = requests.get(url)
        resp.raise_for_status()

        cards = resp.json()["cards"]["mainboard"]
        timestamp = season_start_dates.get(season_id, "1970-01-01")

        for card in cards:
            all_cards.append({
                "season_id": season_id,
                "timestamp": timestamp,
                "scryfallId": card.get("cardID"),
                "tags": card.get("tags", [])
            })

    return all_cards


def export_mainboard_csv(mainboard_data: List[Dict], output_csv: str = "data/processed/cube_mainboard.csv"):
    """
    Export cube mainboard snapshot data to CSV.
    """
    ensure_dir(output_csv)

    df = pd.DataFrame(mainboard_data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.sort_values(["season_id", "timestamp", "scryfallId"], inplace=True)

    df.to_csv(output_csv, index=False)
    print(f"Mainboard snapshot CSV saved: {output_csv}")


def fetch_all_cube_histories(season_data: Dict, output_dir: str = "data/processed/history"):
    """
    Fetch and save cube history JSONs for all seasons from CubeCobra.
    """
    ensure_dir(output_dir)

    for season_id, meta in season_data.items():
        cube_id = meta["cube_id"]
        url = f"https://cubecobra.com/public/cube/history/{cube_id}"

        resp = requests.post(url, json={}, headers={"Content-Type": "application/json"})
        output_path = os.path.join(output_dir, f"{season_id}_history.json")

        if resp.ok:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(resp.json(), f, indent=2, ensure_ascii=False)
            print(f"Saved history for {season_id} → {output_path}")
        else:
            print(f"Failed to fetch history for {season_id}: {resp.status_code}")


def load_combined_history_data(history_dir: str = "data/processed/history") -> List[Tuple[str, Dict]]:
    """
    Load all CubeCobra history JSONs and annotate each post with its season.
    """
    all_posts = []

    for filename in sorted(os.listdir(history_dir)):
        if not filename.endswith("_history.json"):
            continue

        season_id = filename.replace("_history.json", "")
        file_path = os.path.join(history_dir, filename)

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data.get("posts"), list):
            raise ValueError(f"Invalid 'posts' list in {filename}")

        all_posts.extend((season_id, post) for post in data["posts"])

    print(f"Loaded {len(all_posts)} history entries from {history_dir}")
    return all_posts


def parse_timestamp_ms(ms: Optional[int]) -> Optional[str]:
    """Convert milliseconds timestamp to ISO date string."""
    if not ms:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date().isoformat()


def process_history_entries(posts_with_season: List[Tuple[str, Dict]]) -> List[Dict]:
    """
    Convert CubeCobra changelogs to flattened change events with timestamps and types.
    """
    records = []

    for season_id, post in posts_with_season:
        timestamp = parse_timestamp_ms(post.get("date"))
        if not timestamp:
            continue

        changes = post.get("changelog", {}).get("mainboard", {})

        for change_type in ["adds", "removes"]:
            for entry in changes.get(change_type, []):
                card = (
                    entry.get("oldCard") if change_type == "removes"
                    else entry.get("newCard", entry)
                )

                if not isinstance(card, dict) or "cardID" not in card:
                    continue

                records.append({
                    "season_id": season_id,
                    "timestamp": timestamp,
                    "change_type": change_type,
                    "scryfallId": card["cardID"]
                })

    return records


def export_history_csv(history_data: List[Dict], output_csv: str = "data/processed/cube_history.csv"):
    """
    Export changelog history data to a CSV with sorted rows and incremental change IDs.
    """
    ensure_dir(output_csv)

    df = pd.DataFrame(history_data)

    if "season" in df.columns:
        df.rename(columns={"season": "season_id"}, inplace=True)

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.sort_values("timestamp", inplace=True)
    df.insert(0, "change_id", range(1, len(df) + 1))

    df.to_csv(output_csv, index=False)
    print(f"History changes CSV saved: {output_csv}")

import os
import pandas as pd
from datetime import datetime
from collections import defaultdict

from manacore.standings.standings_calculator import load_match_data
from manacore.statistics.statistics_calculator import *

def test_complex_card_availability():
    # Earlier adds on 2021-05-05
    initial_changes = [
        {'change_id': 1, 'season_id': 'Season-1', 'timestamp': '2021-05-05', 'change_type': 'adds', 'scryfallId': '74af08d9-76ef-48aa-86c5-b92ea6e10c9f'},
        {'change_id': 2, 'season_id': 'Season-1', 'timestamp': '2021-05-05', 'change_type': 'adds', 'scryfallId': '207d5f12-fbc6-4c75-9d84-fd2f0023fbd0'},
        {'change_id': 3, 'season_id': 'Season-1', 'timestamp': '2021-05-05', 'change_type': 'adds', 'scryfallId': '2f8f22fb-7291-4517-9b15-e98501f2856b'},
        {'change_id': 4, 'season_id': 'Season-1', 'timestamp': '2021-05-05', 'change_type': 'adds', 'scryfallId': '3178e55c-7e49-4621-906a-a66e5656e276'},
    ]

    # Adds and removes on 2021-05-17
    mid_changes = [
        {'change_id': 548, 'season_id': 'Season-1', 'timestamp': '2021-05-17', 'change_type': 'adds', 'scryfallId': 'ac07e230-0297-4e1d-bdfe-119010e0ad8e'},
        {'change_id': 549, 'season_id': 'Season-1', 'timestamp': '2021-05-17', 'change_type': 'removes', 'scryfallId': '9fc9b802-3381-4318-a352-d85451aba586'},
        {'change_id': 550, 'season_id': 'Season-1', 'timestamp': '2021-05-17', 'change_type': 'adds', 'scryfallId': '07246783-d475-4f61-99ac-e2b574072349'},
        {'change_id': 551, 'season_id': 'Season-1', 'timestamp': '2021-05-17', 'change_type': 'removes', 'scryfallId': '02747420-c635-4f0f-9888-81cf4f7dfc91'},
        {'change_id': 552, 'season_id': 'Season-1', 'timestamp': '2021-05-17', 'change_type': 'removes', 'scryfallId': '01cb2a12-4c1c-413d-91b1-574e4a00e251'},
        {'change_id': 553, 'season_id': 'Season-1', 'timestamp': '2021-05-17', 'change_type': 'removes', 'scryfallId': '000db964-719d-4532-9225-35658565b35d'},
        {'change_id': 554, 'season_id': 'Season-1', 'timestamp': '2021-05-17', 'change_type': 'removes', 'scryfallId': '066cee3d-1bc9-43bb-a1e5-70256875eb9b'},
        {'change_id': 555, 'season_id': 'Season-1', 'timestamp': '2021-05-17', 'change_type': 'adds', 'scryfallId': '064310bd-4a37-4774-b3b4-e7a163571040'},
        {'change_id': 556, 'season_id': 'Season-1', 'timestamp': '2021-05-17', 'change_type': 'adds', 'scryfallId': '101f135f-1404-4333-946e-62d3aa919a62'},
        {'change_id': 557, 'season_id': 'Season-1', 'timestamp': '2021-05-17', 'change_type': 'adds', 'scryfallId': '3f4a16c3-0c10-4c91-901e-2bea4f5dcdca'},
    ]

    cube_history_data = initial_changes + mid_changes
    cube_history_df = pd.DataFrame(cube_history_data)
    cube_history_df['timestamp'] = pd.to_datetime(cube_history_df['timestamp'])

    # Drafts before and after the 2021-05-17 changes
    drafts_data = [
        {'season_id': 'Season-1', 'draft_id': 1001, 'timestamp': '2021-05-10 10:00:00'},  # Before 05-17 changes
        {'season_id': 'Season-1', 'draft_id': 1002, 'timestamp': '2021-05-18 10:00:00'},  # After 05-17 changes
    ]
    drafts_df = pd.DataFrame(drafts_data)
    drafts_df['timestamp'] = pd.to_datetime(drafts_df['timestamp'])

    availability_map = build_card_availability_map(cube_history_df, drafts_df)

    print("Availability map:")
    for season, drafts in availability_map.items():
        print(f"Season {season}:")
        for draft_id, cards in drafts.items():
            print(f"  Draft {draft_id}:")
            for card in cards:
                print(f"    {card}")

    # Assertions:

    # Before 2021-05-17 changes, all initial cards are available, none of the adds/removes on 17th applied yet
    expected_before = {
        '74af08d9-76ef-48aa-86c5-b92ea6e10c9f',
        '207d5f12-fbc6-4c75-9d84-fd2f0023fbd0',
        '2f8f22fb-7291-4517-9b15-e98501f2856b',
        '3178e55c-7e49-4621-906a-a66e5656e276',
    }
    assert availability_map['Season-1'][1001] == expected_before, "Draft 1001 availability mismatch"

    # After 2021-05-17 changes, card removals and adds should be applied
    # Note: Cards removed may not be in initial set (they are removed anyway, no effect)
    expected_after = expected_before.union({
        'ac07e230-0297-4e1d-bdfe-119010e0ad8e',
        '07246783-d475-4f61-99ac-e2b574072349',
        '064310bd-4a37-4774-b3b4-e7a163571040',
        '101f135f-1404-4333-946e-62d3aa919a62',
        '3f4a16c3-0c10-4c91-901e-2bea4f5dcdca',
    })
    # Cards removed on 05-17 that were not in initial set don't affect union
    # The cards removed that are NOT in expected_before can be ignored as they won't be in set

    assert availability_map['Season-1'][1002] == expected_after, "Draft 1002 availability mismatch"

    print("Complex test passed!")

# Run the test
test_complex_card_availability()

# Sample decks data: cards included in drafts by season and draft id
decks_df = pd.DataFrame([
    {'season_id': 'Season-1', 'draft_id': 1, 'scryfallId': 'cardA'},
    {'season_id': 'Season-1', 'draft_id': 2, 'scryfallId': 'cardA'},
    {'season_id': 'Season-1', 'draft_id': 2, 'scryfallId': 'cardB'},
    {'season_id': 'Season-1', 'draft_id': 3, 'scryfallId': 'cardC'},
    {'season_id': 'Season-2', 'draft_id': 4, 'scryfallId': 'cardA'},
])

# Drafts for each season
drafts_df = pd.DataFrame([
    {'season_id': 'Season-1', 'draft_id': 1},
    {'season_id': 'Season-1', 'draft_id': 2},
    {'season_id': 'Season-1', 'draft_id': 3},
    {'season_id': 'Season-2', 'draft_id': 4},
    {'season_id': 'Season-2', 'draft_id': 5},
])

# Card availability: all cards available in each draft for simplicity
card_availability_map = {
    'Season-1': {
        1: {'cardA', 'cardB', 'cardC'},
        2: {'cardA', 'cardB', 'cardC'},
        3: {'cardA', 'cardB', 'cardC'},
    },
    'Season-2': {
        4: {'cardA', 'cardD'},
        5: {'cardA', 'cardD'},
    }
}

df_result = calculate_card_mainboard_rate_per_season(decks_df, drafts_df, card_availability_map)
print(df_result)


# Test functions with prints

def test_no_matches():
    print("\nRunning test_no_matches...")
    matches_df = pd.DataFrame(columns=['season_id', 'draft_id', 'player1', 'player2', 'player1Wins', 'player2Wins'])
    decks_df = pd.DataFrame([
        {'season_id': 'S1', 'draft_id': 1, 'player': 'Alice', 'scryfallId': 'card1'}
    ])
    card_availability_map = {'S1': {1: {'card1'}}}

    df = calculate_card_match_winrate_per_season(matches_df, decks_df, card_availability_map)
    print(df)

def test_no_players_with_card():
    print("\nRunning test_no_players_with_card...")
    matches_df = pd.DataFrame([
        {'season_id': 'S1', 'draft_id': 1, 'player1': 'Bob', 'player2': 'Charlie', 'player1Wins': 2, 'player2Wins': 1}
    ])
    decks_df = pd.DataFrame([
        {'season_id': 'S1', 'draft_id': 1, 'player': 'Alice', 'scryfallId': 'card1'}
    ])
    card_availability_map = {'S1': {1: {'card1'}}}

    df = calculate_card_match_winrate_per_season(matches_df, decks_df, card_availability_map)
    print(df)

def test_one_match_win_and_loss():
    print("\nRunning test_one_match_win_and_loss...")
    matches_df = pd.DataFrame([
        {'season_id': 'S1', 'draft_id': 1, 'player1': 'Alice', 'player2': 'Bob', 'player1Wins': 2, 'player2Wins': 0},
        {'season_id': 'S1', 'draft_id': 1, 'player1': 'Charlie', 'player2': 'Alice', 'player1Wins': 1, 'player2Wins': 2},
    ])
    decks_df = pd.DataFrame([
        {'season_id': 'S1', 'draft_id': 1, 'player': 'Alice', 'scryfallId': 'card1'},
        {'season_id': 'S1', 'draft_id': 1, 'player': 'Bob', 'scryfallId': 'card2'},
        {'season_id': 'S1', 'draft_id': 1, 'player': 'Charlie', 'scryfallId': 'card3'}
    ])
    card_availability_map = {'S1': {1: {'card1', 'card2', 'card3'}}}

    df = calculate_card_match_winrate_per_season(matches_df, decks_df, card_availability_map)
    print(df)

def test_card_not_available():
    print("\nRunning test_card_not_available...")
    matches_df = pd.DataFrame([
        {'season_id': 'S1', 'draft_id': 1, 'player1': 'Alice', 'player2': 'Bob', 'player1Wins': 2, 'player2Wins': 0},
    ])
    decks_df = pd.DataFrame([
        {'season_id': 'S1', 'draft_id': 1, 'player': 'Alice', 'scryfallId': 'card1'},
    ])
    card_availability_map = {'S1': {1: set()}}  # card1 not available

    df = calculate_card_match_winrate_per_season(matches_df, decks_df, card_availability_map)
    print(df)
import pandas as pd

def test_game_winrate_no_matches():
    print("\nTest: No matches")
    matches_df = pd.DataFrame(columns=['season_id', 'draft_id', 'player1Wins', 'player2Wins', 'draws'])
    decks_df = pd.DataFrame([
        {'season_id': 'S1', 'draft_id': 1, 'scryfallId': 'cardA'}
    ])
    card_availability_map = {'S1': {1: {'cardA'}}}
    result = calculate_card_game_winrate_per_season(matches_df, decks_df, card_availability_map)
    print(result)

def test_game_winrate_simple_case():
    print("\nTest: Simple case with one match")
    matches_df = pd.DataFrame([
        {'season_id': 'S1', 'draft_id': 1, 'player1Wins': 2, 'player2Wins': 1, 'draws': 0},
        {'season_id': 'S1', 'draft_id': 1, 'player1Wins': 0, 'player2Wins': 2, 'draws': 1},
    ])
    decks_df = pd.DataFrame([
        {'season_id': 'S1', 'draft_id': 1, 'scryfallId': 'cardA'}
    ])
    card_availability_map = {'S1': {1: {'cardA'}}}
    result = calculate_card_game_winrate_per_season(matches_df, decks_df, card_availability_map)
    print(result)

def test_game_winrate_card_not_available():
    print("\nTest: Card not available in draft")
    matches_df = pd.DataFrame([
        {'season_id': 'S1', 'draft_id': 1, 'player1Wins': 3, 'player2Wins': 2, 'draws': 0}
    ])
    decks_df = pd.DataFrame([
        {'season_id': 'S1', 'draft_id': 1, 'scryfallId': 'cardA'}
    ])
    card_availability_map = {'S1': {1: set()}}  # cardA not available
    result = calculate_card_game_winrate_per_season(matches_df, decks_df, card_availability_map)
    print(result)


# Run all tests
test_no_matches()
test_no_players_with_card()
test_one_match_win_and_loss()
test_card_not_available()
test_game_winrate_no_matches()
test_game_winrate_simple_case()
test_game_winrate_card_not_available()

# Sample match data
matches_data = pd.DataFrame([
    {'season_id': 'Season-1', 'draft_id': 1, 'player1': 'Alice', 'player2': 'Bob', 'player1Wins': 2, 'player2Wins': 1, 'draws': 0},
    {'season_id': 'Season-1', 'draft_id': 2, 'player1': 'Alice', 'player2': 'Charlie', 'player1Wins': 0, 'player2Wins': 2, 'draws': 0},
    {'season_id': 'Season-1', 'draft_id': 3, 'player1': 'Bob', 'player2': 'Charlie', 'player1Wins': 1, 'player2Wins': 2, 'draws': 0},
    {'season_id': 'Season-2', 'draft_id': 4, 'player1': 'Alice', 'player2': 'Bob', 'player1Wins': 2, 'player2Wins': 0, 'draws': 0},
])

# Sample decks data with archetypes at level 2
decks_data = pd.DataFrame([
    {'draft_id': 1, 'player': 'Alice', 'archetype_level_2': 'Aggro'},
    {'draft_id': 1, 'player': 'Bob', 'archetype_level_2': 'Control'},
    {'draft_id': 2, 'player': 'Alice', 'archetype_level_2': 'Aggro'},
    {'draft_id': 2, 'player': 'Charlie', 'archetype_level_2': 'Midrange'},
    {'draft_id': 3, 'player': 'Bob', 'archetype_level_2': 'Control'},
    {'draft_id': 3, 'player': 'Charlie', 'archetype_level_2': 'Midrange'},
    {'draft_id': 4, 'player': 'Alice', 'archetype_level_2': 'Aggro'},
    {'draft_id': 4, 'player': 'Bob', 'archetype_level_2': 'Control'},
])

# Call the function and print results
winrate_df = calculate_archetype_match_winrate(matches_data, decks_data)
print(winrate_df)

matches_data = pd.DataFrame([
    {'season_id': 'Season-1', 'draft_id': 1, 'player1': 'Alice', 'player2': 'Bob', 'player1Wins': 2, 'player2Wins': 1, 'draws': 0},
    {'season_id': 'Season-1', 'draft_id': 2, 'player1': 'Alice', 'player2': 'Charlie', 'player1Wins': 0, 'player2Wins': 2, 'draws': 0},
    {'season_id': 'Season-1', 'draft_id': 3, 'player1': 'Bob', 'player2': 'Charlie', 'player1Wins': 1, 'player2Wins': 2, 'draws': 0},
    {'season_id': 'Season-2', 'draft_id': 4, 'player1': 'Alice', 'player2': 'Bob', 'player1Wins': 2, 'player2Wins': 0, 'draws': 0},
])

# Sample decks data with archetype_level_2
decks_data = pd.DataFrame([
    {'draft_id': 1, 'player': 'Alice', 'season_id': 'Season-1', 'archetype_level_2': 'Aggro'},
    {'draft_id': 1, 'player': 'Bob', 'season_id': 'Season-1', 'archetype_level_2': 'Control'},
    {'draft_id': 2, 'player': 'Alice', 'season_id': 'Season-1', 'archetype_level_2': 'Aggro'},
    {'draft_id': 2, 'player': 'Charlie', 'season_id': 'Season-1', 'archetype_level_2': 'Midrange'},
    {'draft_id': 3, 'player': 'Bob', 'season_id': 'Season-1', 'archetype_level_2': 'Control'},
    {'draft_id': 3, 'player': 'Charlie', 'season_id': 'Season-1', 'archetype_level_2': 'Midrange'},
    {'draft_id': 4, 'player': 'Alice', 'season_id': 'Season-2', 'archetype_level_2': 'Aggro'},
    {'draft_id': 4, 'player': 'Bob', 'season_id': 'Season-2', 'archetype_level_2': 'Control'},
])

# Call the function
winrate_df = calculate_archetype_game_winrate(matches_data, decks_data)

print(winrate_df)


def test_calculate_most_picked_card_by_player_with_given_data():
    data = {
        "season_id": ["Season-4", "Season-4", "Season-4", "Season-4", "Season-4", "Season-4", "Season-4"],
        "draft_id": [20240922, 20240922, 20240923, 20240923, 20240924, 20240924, 20240925],
        "player": ["Guy", "Guy", "Alice", "Alice", "Bob", "Bob", "Bob"],
        "archetype": ["Combo", "Combo", "Control", "Control", "Aggro", "Aggro", "Aggro"],
        "decktype": ["Reanimator", "Reanimator", "Blue-White", "Blue-White", "Red", "Red", "Red"],
        "scryfallId": [
            "23b95b26-0bbf-4fa1-80c1-f621f1a1b947",
            "a7a8b6b8-b95f-4014-b17a-a6d44d965995",
            "a7a8b6b8-b95f-4014-b17a-a6d44d965995",
            "a7a8b6b8-b95f-4014-b17a-a6d44d965995",
            "a7a8b6b8-b95f-4014-b17a-a6d44d965995",
            "23b95b26-0bbf-4fa1-80c1-f621f1a1b947",
            "a7a8b6b8-b95f-4014-b17a-a6d44d965995"
        ],
    }

    decks_df = pd.DataFrame(data)

    # Run the function
    most_picked = calculate_most_picked_card_by_player(decks_df)

    print(most_picked)

# Run the test
test_calculate_most_picked_card_by_player_with_given_data()


def test_calculate_decktype_match_winrate():
    # Sample matches data
    matches_df = pd.DataFrame([
        {'draft_id': 'd1', 'season_id': 'Season-1', 'player1': 'Alice', 'player2': 'Bob', 'player1Wins': 2, 'player2Wins': 1},
        {'draft_id': 'd2', 'season_id': 'Season-1', 'player1': 'Charlie', 'player2': 'Alice', 'player1Wins': 0, 'player2Wins': 2},
        {'draft_id': 'd3', 'season_id': 'Season-1', 'player1': 'Bob', 'player2': 'Charlie', 'player1Wins': 1, 'player2Wins': 2},
    ])

    # Sample decks data with decktype_level_2
    decks_df = pd.DataFrame([
        {'draft_id': 'd1', 'player': 'Alice', 'decktype_level_2': 'Aggro'},
        {'draft_id': 'd1', 'player': 'Bob', 'decktype_level_2': 'Control'},
        {'draft_id': 'd2', 'player': 'Charlie', 'decktype_level_2': 'Midrange'},
        {'draft_id': 'd2', 'player': 'Alice', 'decktype_level_2': 'Aggro'},
        {'draft_id': 'd3', 'player': 'Bob', 'decktype_level_2': 'Control'},
        {'draft_id': 'd3', 'player': 'Charlie', 'decktype_level_2': 'Midrange'},
    ])

    # Call the function
    result = calculate_decktype_match_winrate(matches_df, decks_df, 'decktype_level_2')

    print(result)

# Run the test
test_calculate_decktype_match_winrate()


def test_calculate_decktype_game_winrate():
    matches_df = pd.DataFrame([
        {'draft_id': 'd1', 'player1': 'Alice', 'player2': 'Bob', 'player1Wins': 2, 'player2Wins': 1},
        {'draft_id': 'd2', 'player1': 'Charlie', 'player2': 'Alice', 'player1Wins': 0, 'player2Wins': 2},
        {'draft_id': 'd3', 'player1': 'Bob', 'player2': 'Charlie', 'player1Wins': 1, 'player2Wins': 2},
    ])

    decks_df = pd.DataFrame([
        {'draft_id': 'd1', 'player': 'Alice', 'decktype_level_2': 'Aggro'},
        {'draft_id': 'd1', 'player': 'Bob', 'decktype_level_2': 'Control'},
        {'draft_id': 'd2', 'player': 'Charlie', 'decktype_level_2': 'Midrange'},
        {'draft_id': 'd2', 'player': 'Alice', 'decktype_level_2': 'Aggro'},
        {'draft_id': 'd3', 'player': 'Bob', 'decktype_level_2': 'Control'},
        {'draft_id': 'd3', 'player': 'Charlie', 'decktype_level_2': 'Midrange'},
    ])

    result = calculate_decktype_game_winrate(matches_df, decks_df, 'decktype_level_2')
    print(result)

test_calculate_decktype_game_winrate()
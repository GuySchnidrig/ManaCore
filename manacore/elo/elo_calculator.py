import csv
import pandas as pd
from datetime import datetime
from collections import defaultdict
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Set
from manacore.config.get_seasons import load_season_config, get_season_for_date


@dataclass
class MatchResult:
    """Represents the result of a match between two players."""
    player1: str
    player2: str
    p1_wins: int
    p2_wins: int
    draws: int
    
    @property
    def total_games(self) -> int:
        return self.p1_wins + self.p2_wins + self.draws
    
    @property
    def player1_score(self) -> float:
        """Calculate player1's score as a fraction (0.0 to 1.0)."""
        if self.total_games == 0:
            return 0.5
        return (self.p1_wins + 0.5 * self.draws) / self.total_games
    
    @property
    def dominance_modifier(self) -> float:
        """Calculate the dominance modifier based on match result."""
        if self.p1_wins == 2 and self.p2_wins == 0:
            return 1.0  # dominant win
        elif self.p1_wins == 2 and self.p2_wins == 1:
            return 0.67  # narrow win
        elif self.p2_wins == 2 and self.p1_wins == 0:
            return 1.0  # dominant loss
        elif self.p2_wins == 2 and self.p1_wins == 1:
            return 0.33  # narrow loss
        else:
            return 1.0  # other cases (draws, etc.)


@dataclass
class EloProgress:
    """Represents a player's Elo progress entry."""
    season_id: str
    draft_id: str
    player_name: str
    matches_played: int
    elo: float
    rating_change: float


class EloRatingSystem:
    """Manages Elo ratings for players across drafts and seasons."""
    
    def __init__(self, k_factor: int = 32, default_rating: float = 1000.0):
        self.k_factor = k_factor
        self.default_rating = default_rating
        self.ratings: Dict[str, float] = defaultdict(lambda: default_rating)
        self.season_config = load_season_config()
        self.elo_progress: List[EloProgress] = []
        self.matches_played_per_draft: Dict[Tuple[str, str], int] = defaultdict(int)
        
    def load_latest_elos(self, elo_history_file: str) -> None:
        """
        Load the latest Elo ratings for players from a wide-format Elo history CSV file.
        
        Parameters:
        -----------
        elo_history_file : str
            Path to a CSV file with columns: 'player', 'baseElo', and draft columns like 'S1D1', ..., 'S3D12'.
        """
        try:
            df = pd.read_csv(elo_history_file, encoding='utf-8')
            
            # Clean up BOM and whitespace
            df.columns = df.columns.str.lstrip('\ufeff').str.strip()
            
            # Get draft columns (columns starting with 'S')
            draft_columns = [col for col in df.columns if col.startswith('S')]
            
            for _, row in df.iterrows():
                player = str(row['player']).strip()
                
                # Find the latest non-empty draft Elo value
                draft_values = [row[col] for col in draft_columns if pd.notna(row[col]) and row[col] != '']
                
                if draft_values:
                    latest_elo = float(draft_values[-1])
                else:
                    latest_elo = float(row.get('baseElo', self.default_rating))
                
                self.ratings[player] = latest_elo
                
        except FileNotFoundError:
            print(f"Warning: Elo history file {elo_history_file} not found. Using default ratings.")
        except Exception as e:
            print(f"Error loading Elo history: {e}. Using default ratings.")
    
    def expected_score(self, rating_a: float, rating_b: float) -> float:
        """
        Calculate the expected probability that Player A wins against Player B based on Elo ratings.
        
        Parameters:
        -----------
        rating_a : float
            Elo rating of Player A.
        rating_b : float
            Elo rating of Player B.
            
        Returns:
        --------
        float
            Expected score (probability) of Player A winning, a value between 0 and 1.
        """
        return 1 / (1 + 10 ** ((rating_b - rating_a) / 400))
    
    def update_elo(self, rating_a: float, rating_b: float, score_a: float, modifier: float = 1.0) -> Tuple[float, float]:
        """
        Update the Elo ratings of two players after a match.
        
        Parameters:
        -----------
        rating_a : float
            Current Elo rating of Player A.
        rating_b : float
            Current Elo rating of Player B.
        score_a : float
            Actual score of Player A in the match (0.0 to 1.0).
        modifier : float
            Dominance modifier to adjust rating changes.
            
        Returns:
        --------
        tuple (float, float)
            Updated Elo ratings for Player A and Player B, respectively.
        """
        expected_a = self.expected_score(rating_a, rating_b)
        expected_b = 1 - expected_a
        score_b = 1 - score_a
        
        # Calculate base rating changes
        change_a = self.k_factor * (score_a - expected_a)
        change_b = self.k_factor * (score_b - expected_b)
        
        # Apply modifier
        change_a *= modifier
        change_b *= modifier
        
        new_rating_a = rating_a + change_a
        new_rating_b = rating_b + change_b
        
        return new_rating_a, new_rating_b, change_a, change_b
    
    def process_match(self, match: MatchResult, draft_id: str, season_id: str) -> None:
        """Process a single match and update player ratings."""
        # Update match counts
        self.matches_played_per_draft[(draft_id, match.player1)] += 1
        self.matches_played_per_draft[(draft_id, match.player2)] += 1
        
        # Get current ratings
        rating_p1 = self.ratings[match.player1]
        rating_p2 = self.ratings[match.player2]
        
        # Calculate new ratings with dominance modifier
        new_rating_p1, new_rating_p2, change_p1, change_p2 = self.update_elo(
            rating_p1, rating_p2, match.player1_score, match.dominance_modifier
        )
        
        # Update stored ratings
        self.ratings[match.player1] = new_rating_p1
        self.ratings[match.player2] = new_rating_p2
        
        # Record progress
        self.elo_progress.extend([
            EloProgress(
                season_id=season_id,
                draft_id=draft_id,
                player_name=match.player1,
                matches_played=self.matches_played_per_draft[(draft_id, match.player1)],
                elo=new_rating_p1,
                rating_change=change_p1
            ),
            EloProgress(
                season_id=draft_id,
                draft_id=draft_id,
                player_name=match.player2,
                matches_played=self.matches_played_per_draft[(draft_id, match.player2)],
                elo=new_rating_p2,
                rating_change=change_p2
            )
        ])
    
    def add_non_participants(self, draft_id: str, season_id: str, participating_players: Set[str]) -> None:
        """Add entries for players who didn't participate in this draft."""
        all_players = set(self.ratings.keys())
        non_participants = all_players - participating_players
        
        for player in non_participants:
            self.elo_progress.append(EloProgress(
                season_id=season_id,
                draft_id=draft_id,
                player_name=player,
                matches_played=0,
                elo=self.ratings[player],
                rating_change=0.0
            ))
    
    def process_matches_from_csv(self, csv_file: str) -> None:
        """
        Process all matches from a CSV file and update Elo ratings.
        
        Parameters:
        -----------
        csv_file : str
            Path to the input CSV file containing match records.
        """
        try:
            df = pd.read_csv(csv_file)
            
            # Validate required columns
            required_columns = ['draft_id', 'player1', 'player2', 'player1Wins', 'player2Wins', 'draws']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            current_draft = None
            current_season = None
            draft_participants: Set[str] = set()
            
            for _, row in df.iterrows():
                draft_id = str(row['draft_id'])
                
                # Parse season from draft date
                try:
                    draft_date = datetime.strptime(draft_id, "%Y%m%d").date()
                    season_id = get_season_for_date(draft_date, self.season_config)
                    if season_id:
                        current_season = season_id
                except (ValueError, TypeError):
                    # Keep previous season if date parsing fails
                    pass
                
                # Handle draft transition
                if current_draft and draft_id != current_draft:
                    self.add_non_participants(current_draft, current_season or "Unknown Season", draft_participants)
                    draft_participants = set()
                
                current_draft = draft_id
                
                # Create match result
                match = MatchResult(
                    player1=str(row['player1']).strip(),
                    player2=str(row['player2']).strip(),
                    p1_wins=int(row['player1Wins']),
                    p2_wins=int(row['player2Wins']),
                    draws=int(row['draws'])
                )
                
                draft_participants.update([match.player1, match.player2])
                
                # Process the match
                self.process_match(match, draft_id, current_season or "Unknown Season")
            
            # Handle the last draft
            if current_draft:
                self.add_non_participants(current_draft, current_season or "Unknown Season", draft_participants)
                
        except FileNotFoundError:
            raise FileNotFoundError(f"Match data file {csv_file} not found")
        except Exception as e:
            raise RuntimeError(f"Error processing matches: {e}")
    
    def save_progress_to_csv(self, output_file: str) -> None:
        """
        Save the Elo progress to a CSV file.
        
        Parameters:
        -----------
        output_file : str
            Path to the output CSV file.
        """
        try:
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_file, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(['season_id', 'draft_id', 'player_name', 'matches_played', 'elo', 'rating_change'])
                
                for progress in self.elo_progress:
                    writer.writerow([
                        progress.season_id,
                        progress.draft_id,
                        progress.player_name,
                        progress.matches_played,
                        round(progress.elo, 4),
                        round(progress.rating_change, 4)
                    ])
            
            print(f"Wrote {len(self.elo_progress)} rows to {output_file}")
            
        except Exception as e:
            raise RuntimeError(f"Error saving progress to CSV: {e}")
    
    def get_current_ratings(self) -> Dict[str, float]:
        """Get the current ratings for all players."""
        return dict(self.ratings)
    
    def get_player_rating(self, player: str) -> float:
        """Get the current rating for a specific player."""
        return self.ratings[player]


def process_matches(csv_file: str, output_file: str, elo_history_file: str = "data/raw/elo_history.csv") -> None:
    """
    Main function to process match data and generate Elo progression.
    
    Parameters:
    -----------
    csv_file : str
        Path to the input CSV file containing match records.
    output_file : str
        Path to the output CSV file where Elo rating progress will be saved.
    elo_history_file : str, optional
        Path to the Elo history file to load initial ratings from.
    """
    try:
        # Initialize the Elo system
        elo_system = EloRatingSystem()
        
        elo_system.load_latest_elos(elo_history_file)
        
        elo_system.process_matches_from_csv(csv_file)
        
        elo_system.save_progress_to_csv(output_file)
        
        print(f"Successfully processed matches and saved Elo progression to {output_file}")
        
    except Exception as e:
        print(f"Error processing matches: {e}")
        raise

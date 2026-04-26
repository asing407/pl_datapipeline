import requests
import os
import pandas as pd
from dotenv import load_dotenv
import time
import logging
from typing import List, Dict
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fetch_matches.log'),  # Fixed: no nested path
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

SPORTS_API_KEY = os.getenv("SPORTS_API_KEY")
BASE_URL = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": SPORTS_API_KEY}

# Retry configuration
def get_retry_session(retries=3, backoff_factor=0.5):
    """Create requests session with automatic retry logic"""
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=(500, 502, 504, 429)
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def validate_match_record(match: Dict) -> bool:
    """Validate that a match record has all required fields"""
    required_fields = ['fixture', 'teams', 'goals']
    
    try:
        for field in required_fields:
            if field not in match:
                logger.warning(f"Missing required field: {field}")
                return False
        
        # Validate nested fields
        if not match['teams'].get('home') or not match['teams'].get('away'):
            logger.warning("Missing team information")
            return False
            
        if match['goals'].get('home') is None or match['goals'].get('away') is None:
            logger.warning("Missing goals information")
            return False
            
        return True
        
    except Exception as e:
        logger.error(f"Validation error: {str(e)}")
        return False

def fetch_matchday(session: requests.Session, matchday: int, max_retries: int = 3) -> List[Dict]:
    """Fetch matches for a specific matchday with retry logic"""
    
    params = {
        "league": "39",
        "season": "2023",
        "round": f"Regular Season - {matchday}"
    }
    
    for attempt in range(max_retries):
        try:
            response = session.get(
                f"{BASE_URL}/fixtures",
                headers=HEADERS,
                params=params,
                timeout=10
            )
            
            response.raise_for_status()
            
            data = response.json()
            
            # Validate response structure
            if "response" not in data:
                logger.error(f"Matchday {matchday}: Invalid response structure")
                return []
            
            fixtures = data["response"]
            
            # Validate each match record
            valid_fixtures = [f for f in fixtures if validate_match_record(f)]
            
            if len(valid_fixtures) != len(fixtures):
                logger.warning(f"Matchday {matchday}: {len(fixtures) - len(valid_fixtures)} invalid records filtered")
            
            logger.info(f"✅ Matchday {matchday}/38 — {len(valid_fixtures)} matches fetched")
            return valid_fixtures
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                wait_time = (attempt + 1) * 10
                logger.warning(f"Rate limit hit. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
                time.sleep(wait_time)
            else:
                logger.error(f"HTTP error on matchday {matchday}: {str(e)}")
                if attempt == max_retries - 1:
                    return []
                    
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error on matchday {matchday}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep((attempt + 1) * 2)
            else:
                return []
                
        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout on matchday {matchday}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep((attempt + 1) * 2)
            else:
                return []
                
        except Exception as e:
            logger.error(f"Unexpected error on matchday {matchday}: {str(e)}")
            return []
    
    logger.error(f"❌ Matchday {matchday} failed after {max_retries} attempts")
    return []

def parse_match_record(match: Dict) -> Dict:
    """Parse raw API response into structured record"""
    try:
        return {
            "match_id": match["fixture"]["id"],
            "date": match["fixture"]["date"],
            "matchday": match["league"]["round"].split(" - ")[-1] if "league" in match else None,
            "home_team": match["teams"]["home"]["name"],
            "away_team": match["teams"]["away"]["name"],
            "home_goals": match["goals"]["home"],
            "away_goals": match["goals"]["away"],
            "status": match["fixture"]["status"]["long"],
            "venue": match["fixture"]["venue"]["name"] if match["fixture"]["venue"] else "Unknown",
            "referee": match["fixture"]["referee"] if match["fixture"]["referee"] else "Unknown"
        }
    except KeyError as e:
        logger.error(f"Error parsing match record: Missing key {str(e)}")
        return None

def fetch_all_matches() -> List[Dict]:
    """Fetch all Premier League 2023/24 matches with error handling"""
    
    if not SPORTS_API_KEY:
        logger.error("SPORTS_API_KEY not found in environment variables!")
        raise ValueError("Missing API key")
    
    logger.info("=" * 50)
    logger.info("Fetching FULL Premier League 2023/24 Season")
    logger.info("Estimated time: ~5 minutes")
    logger.info("=" * 50)
    
    session = get_retry_session()
    all_records = []
    failed_matchdays = []
    
    for matchday in range(1, 39):
        fixtures = fetch_matchday(session, matchday)
        
        if not fixtures:
            failed_matchdays.append(matchday)
            logger.warning(f"No data retrieved for matchday {matchday}")
            continue
        
        for match in fixtures:
            parsed = parse_match_record(match)
            if parsed:
                all_records.append(parsed)
        
        time.sleep(7)  # Rate limiting
    
    # Summary
    logger.info(f"\n{'=' * 50}")
    logger.info(f"Fetch Summary:")
    logger.info(f"  Total matches: {len(all_records)}")
    logger.info(f"  Expected: 380")
    logger.info(f"  Success rate: {(len(all_records)/380)*100:.1f}%")
    
    if failed_matchdays:
        logger.warning(f"  Failed matchdays: {failed_matchdays}")
    
    logger.info(f"{'=' * 50}\n")
    
    return all_records

def save_matches(records: List[Dict]) -> pd.DataFrame:
    """Save matches to CSV with validation"""
    
    if not records:
        logger.error("No records to save!")
        raise ValueError("Empty records list")
    
    try:
        df = pd.DataFrame(records)
        
        # Data quality checks
        initial_count = len(df)
        df = df.drop_duplicates(subset=['match_id'], keep='first')
        duplicates_removed = initial_count - len(df)
        
        if duplicates_removed > 0:
            logger.warning(f"Removed {duplicates_removed} duplicate records")
        
        # Check for nulls in critical columns
        critical_cols = ['match_id', 'home_team', 'away_team', 'home_goals', 'away_goals']
        null_counts = df[critical_cols].isnull().sum()
        
        if null_counts.any():
            logger.warning(f"Null values found:\n{null_counts[null_counts > 0]}")
        
        # Save to CSV
        output_path = "matches_raw.csv"  # Fixed: save in current directory
        df.to_csv(output_path, index=False)
        
        logger.info(f"✅ Saved {len(df)} matches to {output_path}")
        
        return df
        
    except Exception as e:
        logger.error(f"Error saving matches: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        records = fetch_all_matches()
        df = save_matches(records)
        logger.info("Pipeline completed successfully! 🎉")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
import requests
import os
import pandas as pd
from dotenv import load_dotenv
import time

load_dotenv()

SPORTS_API_KEY = os.getenv("SPORTS_API_KEY")
BASE_URL = "https://v3.football.api-sports.io"
HEADERS = {
    "x-apisports-key": SPORTS_API_KEY
}

def fetch_all_matches():
    all_records = []
    
    for matchday in range(1, 39):  # All 38 matchdays
        params = {
            "league": "39",
            "season": "2023",  # ⚠️ IMPORTANT: 2023 = 2023/24 season
            "round": f"Regular Season - {matchday}"
        }
        
        response = requests.get(
            f"{BASE_URL}/fixtures",
            headers=HEADERS,
            params=params
        )
        
        if response.status_code == 200:
            fixtures = response.json()["response"]
            for m in fixtures:
                all_records.append({
                    "match_id":   m["fixture"]["id"],
                    "date":       m["fixture"]["date"],
                    "matchday":   matchday,
                    "home_team":  m["teams"]["home"]["name"],
                    "away_team":  m["teams"]["away"]["name"],
                    "home_goals": m["goals"]["home"],
                    "away_goals": m["goals"]["away"],
                    "status":     m["fixture"]["status"]["long"],
                    "venue":      m["fixture"]["venue"]["name"],
                    "referee":    m["fixture"]["referee"]
                })
            print(f"✅ Matchday {matchday}/38 — {len(fixtures)} matches fetched")
        else:
            print(f"❌ Matchday {matchday} failed: {response.status_code}")
        
        time.sleep(7)  # Slower to avoid 429
    
    return all_records

def save_matches(records):
    new_df = pd.DataFrame(records)
    
    # Remove duplicates
    new_df = new_df.drop_duplicates(subset=['match_id'], keep='first')
    
    # Overwrite completely
    new_df.to_csv("ingestion/matches_raw.csv", index=False)
    print(f"\n✅ Done! {len(new_df)} total matches saved!")
    return new_df

if __name__ == "__main__":
    print("=" * 50)
    print("Fetching FULL Premier League 2023/24 Season")
    print("Estimated time: ~5 minutes")
    print("=" * 50 + "\n")
    records = fetch_all_matches()
    save_matches(records)
import json
import requests
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

APIFY_TOKEN = os.getenv("APIFY_TOKEN")

if not APIFY_TOKEN:
    raise ValueError("APIFY_TOKEN not found in .env")

url = (
    "https://api.apify.com/v2/acts/"
    "spry_wholemeal~reddit-scraper/"
    "run-sync-get-dataset-items"
)

payload = {
    "includeRaw": False,
    "mode": "search",
    "search": {
        "commentsHighEngagementFilterPosts": False,
        "commentsHighEngagementMinComments": 5,
        "commentsHighEngagementMinScore": 5,
        "commentsMaxDepth": 3,
        "commentsMaxTopLevel": 30,
        "commentsMode": "all",
        "includeNsfw": False,
        "maxPostsPerQuery": 25,
        "queries": [
            "sunday river conditions",
            "sugarbush conditions",
            "killington conditions",
            "loon conditions",
            "sugarloaf conditions",
            "pico conditions",
            "stratton conditions"
        ],
        "restrictToSubreddit": "icecoast",
        "selfPostsOnly": False,
        "sort": "new",
        "strictEnabled": False,
        "strictTerms": [
            "sunday river conditions",
            "sugarbush conditions",
            "killington conditions",
            "loon conditions",
            "sugarloaf conditions",
            "pico conditions",
            "stratton conditions"
        ],
        "timeframe": "month"
    }
}

# Call Apify
resp = requests.post(
    f"{url}?token={APIFY_TOKEN}",
    json=payload,
    timeout=360
)
resp.raise_for_status()

items = resp.json()
print(f"Returned {len(items)} items")

# Save raw JSON
with open("reddit_apify_results.json", "w", encoding="utf-8") as f:
    json.dump(items, f, ensure_ascii=False, indent=2)

# Flatten + clean
rows = []
for item in items:
    rows.append({
        "id": item.get("id"),
        "type": item.get("type"),
        "subreddit": item.get("subreddit"),
        "title": item.get("title"),
        "text": item.get("text") or item.get("body") or item.get("selfText"),
        "score": item.get("score"),
        "numComments": item.get("numComments"),
        "createdAt": item.get("createdAt"),
        "author": item.get("author"),
        "url": item.get("url"),
        "permalink": item.get("permalink"),
    })

df = pd.DataFrame(rows)

# Keep only posts + remove duplicates
df = df[df["type"] == "post"]
df = df.drop_duplicates(subset=["id"])
df = df.reset_index(drop=True)

df.to_csv("reddit_apify_results.csv", index=False)

print("Saved cleaned CSV + JSON")
print(df.head())
# YouTube Metadata Scraper
#
# SETUP STEPS:
# 1. Go to https://console.cloud.google.com/
# 2. Create a project or select an existing one.
# 3. Enable "YouTube Data API v3" under API & Services > Library.
# 4. Go to API & Services > Credentials.
# 5. Click "Create Credentials" > "API Key".
# 6. Copy the API key.
# 7. Create a settings file named "settings.json" in the same directory as this script.
# 8. Required keys in settings.json:
#    - "api_key": Your API key string (required)
#    - "channel_id": Target channel ID (format: UC...) (required)
#    - "data_dir": Output directory for JSON files (optional, default: "data/videos")
#    - "scrape_interval_days": Re-scrape threshold in days (optional, default: 7)
#    - "estimated_daily_quota": Quota limit estimate (optional, default: 10000)
#
# USAGE:
#   python youtube_saver.py              # interactive mode
#   python youtube_saver.py --yes-to-all # batch mode without confirmation

import os
import json
import time
import argparse
from datetime import datetime, timezone, timedelta
from googleapiclient.discovery import build

# LOAD SETTINGS
SETTINGS_FILE = 'settings.json'
if not os.path.exists(SETTINGS_FILE):
    raise FileNotFoundError(f"Missing settings file: {SETTINGS_FILE}")

with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
    settings = json.load(f)

API_KEY = settings.get('api_key')
CHANNEL_ID = settings.get('channel_id')
DATA_DIR = settings.get('data_dir', 'data/videos')
SCRAPE_INTERVAL_DAYS = settings.get('scrape_interval_days', 7)
ESTIMATED_DAILY_QUOTA = settings.get('estimated_daily_quota', 10000)

if not API_KEY or not CHANNEL_ID:
    raise ValueError("Both 'api_key' and 'channel_id' must be set in settings.json")

# INIT YOUTUBE CLIENT
youtube = build('youtube', 'v3', developerKey=API_KEY)

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

quota_usage = 0
state = {'confirm_all': False}

def iso_to_datetime(iso_str):
    # Minimal change: attach UTC timezone to the parsed datetime
    return datetime.strptime(iso_str, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)

def should_scrape(video_id):
    path = os.path.join(DATA_DIR, f'{video_id}.json')
    if not os.path.exists(path):
        print(f'NEW: {video_id} (no local data)')
        return True
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        scraped_at = iso_to_datetime(data.get('scraped_at', '1970-01-01T00:00:00Z'))
        if datetime.now(timezone.utc) - scraped_at > timedelta(days=SCRAPE_INTERVAL_DAYS):
            print(f'UPDATE: {video_id} (older than {SCRAPE_INTERVAL_DAYS}d)')
            return True
        else:
            print(f'SKIP: {video_id} (scraped recently)')
            return False
    except Exception as e:
        print(f'ERROR: {video_id} (failed to read local file: {e})')
        return True

def save_video_data(video_id, metadata, comments):
    data = {
        'video_id': video_id,
        'scraped_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'title': metadata['snippet']['title'],
        'description': metadata['snippet']['description'],
        'published_at': metadata['snippet']['publishedAt'],
        'statistics': metadata.get('statistics', {}),
        'comments': comments
    }
    path = os.path.join(DATA_DIR, f'{video_id}.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def confirm_action(prompt, estimated_cost, auto_yes):
    global quota_usage
    if auto_yes or state.get('confirm_all'):
        quota_usage += estimated_cost
        return True

    print(f'{prompt} [est. quota cost: {estimated_cost}, used: {quota_usage}, used/limit: {quota_usage}/{ESTIMATED_DAILY_QUOTA} ({(quota_usage/ESTIMATED_DAILY_QUOTA)*100:.1f}%)]')
    ans = input('[y]es / [n]o / [a]ll: ').strip().lower()
    if ans == 'a':
        state['confirm_all'] = True
        quota_usage += estimated_cost
        return True
    if ans == 'y':
        quota_usage += estimated_cost
        return True
    if ans == 'n':
        print('Aborted by user.')
        exit(0)
    return False

def get_all_video_ids(auto_yes):
    video_ids = []
    next_page_token = None
    while True:
        if not confirm_action('Request: search.list (list videos)', 100, auto_yes):
            break
        res = youtube.search().list(
            part='id',
            channelId=CHANNEL_ID,
            maxResults=50,
            pageToken=next_page_token,
            type='video'
        ).execute()
        for item in res['items']:
            video_ids.append(item['id']['videoId'])
        next_page_token = res.get('nextPageToken')
        if not next_page_token:
            break
    return video_ids

def get_video_metadata(video_id, auto_yes):
    if not confirm_action(f'Request: videos.list for {video_id}', 1, auto_yes):
        return None
    res = youtube.videos().list(
        part='snippet,statistics',
        id=video_id
    ).execute()
    return res['items'][0] if res['items'] else None

def get_video_comments(video_id, auto_yes):
    comments = []
    next_page_token = None
    while True:
        if not confirm_action(f'Request: commentThreads.list for {video_id}', 1, auto_yes):
            break
        res = youtube.commentThreads().list(
            part='snippet,replies',
            videoId=video_id,
            maxResults=100,
            pageToken=next_page_token,
            textFormat='plainText'
        ).execute()
        for item in res['items']:
            top = item['snippet']['topLevelComment']['snippet']
            comments.append({
                'comment_id': item['id'],
                'author': top['authorDisplayName'],
                'published_at': top['publishedAt'],
                'text': top['textDisplay'],
                'parent_id': None
            })
            if 'replies' in item:
                for reply in item['replies']['comments']:
                    rep = reply['snippet']
                    comments.append({
                        'comment_id': reply['id'],
                        'author': rep['authorDisplayName'],
                        'published_at': rep['publishedAt'],
                        'text': rep['textDisplay'],
                        'parent_id': item['id']
                    })
        next_page_token = res.get('nextPageToken')
        if not next_page_token:
            break
    return comments

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--yes-to-all', action='store_true', help='Automatically confirm all actions')
    args = parser.parse_args()

    auto_yes = args.yes_to_all
    video_ids = get_all_video_ids(auto_yes)
    total_videos = len(video_ids)
    print(f'Found {total_videos} videos.')
    for idx, video_id in enumerate(video_ids, start=1):
        print(f"\nProcessing video {idx}/{total_videos} (Quota used: {quota_usage}/{ESTIMATED_DAILY_QUOTA} daily)")
        if not should_scrape(video_id):
            continue
        try:
            metadata = get_video_metadata(video_id, auto_yes)
            if not metadata:
                print(f'ERROR: {video_id} (metadata not found)')
                continue
            comments = get_video_comments(video_id, auto_yes)
            save_video_data(video_id, metadata, comments)
            print(f'SAVED: {video_id} (Quota used: {quota_usage}/{ESTIMATED_DAILY_QUOTA} daily)')
            time.sleep(1)
        except Exception as e:
            print(f'ERROR: {video_id} (exception: {e})')
    print(f"\nRun completed. Total quota used: {quota_usage}/{ESTIMATED_DAILY_QUOTA} daily.")

if __name__ == '__main__':
    main()

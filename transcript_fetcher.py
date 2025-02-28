import os
import re
import requests
from time import sleep
from pytube import Playlist
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound

# Configuration
MIN_DURATION = 600  # 10 minutes in seconds
RETRY_ATTEMPTS = 3
REQUEST_DELAY = 1 

def generate_episode_data(total_episodes, manual_data=None):
    
    if manual_data:
        return manual_data
    return [{"index": i + 1, "episode": i + 1} for i in range(total_episodes)]


dramas = {
    "Daraar": {
        "link": "https://www.youtube.com/playlist?list=PLdZNFVCDo_1cOWnp-bw3x8CxOw7bMxRt-",
        "episodes": generate_episode_data(40)
    },
    "Baichain Dil": {
        "link": "https://www.youtube.com/playlist?list=PLB1BPYz25JSpGfcskNyX0DmwlXcNOvyT4",
        "episodes": generate_episode_data(37)
    },
    "Main Na Janoo": {
        "link": "https://www.youtube.com/watch?v=5Cun41G44dc&list=PLbVdwtmx18sviyRcmCCQirArY5DR1doQQ&index=34",
        "episodes": generate_episode_data(31)
    },
    "Parizaad": {
        "link": "https://www.youtube.com/watch?v=fwZ6JNfXezg&list=PLbVdwtmx18stXNeBl2fTxbHUsP-HbIYth",
        "episodes": generate_episode_data(29)
    },
    "Qabeel": {
        "link": "https://www.youtube.com/watch?v=4xUvwCzhyQs&list=PLqunGGXHQ5sEsPa8fkFyzvzxUd0e8FRv_&index=1",
        "link": "https://www.youtube.com/watch?v=4xUvwCzhyQs&list=PLqunGGXHQ5sEsPa8fkFyzvzxUd0e8FRv_&index=1",
        # "link": "https://www.youtube.com/watch?v=_p8bCk8pEv4&list=PLb2aaNHUy_gGbfcGbIOIDbWmpXVpurgGh&index=1",
        "episodes": generate_episode_data(1)
    },
    "Aye Ishq E Junoon": {
        # "link": "https://www.youtube.com/watch?v=1HlBsY_7KOE&list=PLz2MrXbUSiBoaGl0Ia2Q-_G8md6k8DegO",
        "link": "https://www.youtube.com/watch?v=_p8bCk8pEv4&list=PLb2aaNHUy_gGbfcGbIOIDbWmpXVpurgGh&index=1",
        "episodes": generate_episode_data(32)
    },
    "Sotan": {
        "link": "https://www.youtube.com/watch?v=1HlBsY_7KOE&list=PLz2MrXbUSiBoaGl0Ia2Q-_G8md6k8DegO",
        # 
        "episodes": generate_episode_data(58)
    },
    "Zard Patton Ka Bunn": {
        "link": "https://www.youtube.com/watch?v=Y3bPhqTEGSY&list=PLbVdwtmx18su3GY_B7miQbxmhbVh9KTDn",
        # 
        "episodes": generate_episode_data(29)
    },
    "Darlings": {
        # "link": "https://www.youtube.com/watch?v=3ZZn3haoRFA&list=PLs2CG9JU32b7iF3Iszyd63vxm47qeYysE",
        "link": "https://www.youtube.com/watch?v=Gr9UyxQYjO4&list=PLQTepLZOvCg5jD7ljW8Eg2C_HJNvGmicV",
        "episodes": generate_episode_data(55)
    },
    "Kaisa Mera Naseeb": {
        "link": "https://www.youtube.com/watch?v=XI8TJxKc3Kw&list=PLz2MrXbUSiBoojRUSDm1dUi4RdUIDtwXa",
        # "link": "https://www.youtube.com/watch?v=FQxDh-pKXj0&list=PLbVdwtmx18sv59ZlGX7qmAj65AXF5iRNu",
        "episodes": generate_episode_data(8)
    },
    "Akhara": {
        "link": "https://www.youtube.com/watch?v=3ZZn3haoRFA&list=PLs2CG9JU32b7iF3Iszyd63vxm47qeYysE",
        # "link": "https://www.youtube.com/watch?v=soj9FDuHBGU&list=PLeb83ChrfOzkYh3FJFiZ5hW8uZj6yaJ79&index=47",
        "episodes": generate_episode_data(34)
    },
    "Mohabbatain Chahatain": {
        "link": "https://www.youtube.com/watch?v=soj9FDuHBGU&list=PLeb83ChrfOzkYh3FJFiZ5hW8uZj6yaJ79&index=47",
        # "link": "https://www.youtube.com/watch?v=8o7xs7MLpQA&list=PLb2aaNHUy_gHLxFkFX4uFSx-P4vxZ7jBr",
        "episodes": generate_episode_data(6)
    },
    "Jaan Se Pyara Juni": {
        "link": "https://www.youtube.com/watch?v=FQxDh-pKXj0&list=PLbVdwtmx18sv59ZlGX7qmAj65AXF5iRNu",
        "episodes": generate_episode_data(34)
    },
    "Me Kahani Hun": {
        "link": "https://www.youtube.com/watch?v=hLRuSVJ_Ynk&list=PLeb83ChrfOzkFzkenCQthTFLgPB5FsLan&index=12",
        "episodes": generate_episode_data(12)
    },
    "Tere Bina Mein Nahi": {
        "link": "https://www.youtube.com/watch?v=8o7xs7MLpQA&list=PLb2aaNHUy_gHLxFkFX4uFSx-P4vxZ7jBr",
        "episodes": generate_episode_data(39)
    },
    "Umm-e-Haniya": {
        "link": "https://www.youtube.com/watch?v=YxIb_BNJkI0&list=PLdZNFVCDo_1cFNYaFX9C5ZuQ3ZkL3nFGT&index=2",
        "episodes": generate_episode_data(38)
    },
    "Besharam": {
        "link": "https://www.youtube.com/watch?v=kLamSiob72Y&list=PL3y6etwW5z8JxbJp64nA4fmsF_7mgeJai",
        "episodes": generate_episode_data(24)
    },
    
}

def get_video_duration(url):
    """Get video duration through HTML parsing"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        for _ in range(RETRY_ATTEMPTS):
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                # Method 1: Search for ISO 8601 duration
                duration_match = re.search(r'"duration":"PT(\d+H)?(\d+M)?(\d+S)?', response.text)
                if duration_match:
                    hours = int(duration_match.group(1)[:-1]) if duration_match.group(1) else 0
                    minutes = int(duration_match.group(2)[:-1]) if duration_match.group(2) else 0
                    seconds = int(duration_match.group(3)[:-1]) if duration_match.group(3) else 0
                    return hours * 3600 + minutes * 60 + seconds
                
                # Method 2: Search for milliseconds duration
                ms_match = re.search(r'"approxDurationMs":"(\d+)"', response.text)
                if ms_match:
                    return int(ms_match.group(1)) // 1000
                    
            sleep(REQUEST_DELAY)
            
    except Exception as e:
        print(f"Duration error: {str(e)}")
    
    # Final fallback
    print("⏱️  Using pytube fallback for duration")
    try:
        from pytube import YouTube
        yt = YouTube(url)
        return yt.length
    except:
        return 0

def get_transcripts(video_id):
    """Get transcripts with auto-translate fallback"""
    try:
        transcripts = YouTubeTranscriptApi.list_transcripts(video_id)
    except (TranscriptsDisabled, NoTranscriptFound):
        return None, None

    en_transcript = ur_transcript = None
    
    # English transcript
    try:
        en_transcript = transcripts.find_transcript(['en'])
    except NoTranscriptFound:
        pass
        
    # Urdu transcript (original or translated)
    try:
        ur_transcript = transcripts.find_transcript(['ur'])
    except NoTranscriptFound:
        if en_transcript:
            try:  # Auto-translate from English
                ur_transcript = en_transcript.translate('ur')
            except:
                pass

    return (
        en_transcript.fetch() if en_transcript else None,
        ur_transcript.fetch() if ur_transcript else None
    )

def process_dramas():
    print("🚀 Starting transcript processing...")
    
    for drama_name, data in dramas.items():
        print(f"\n📺 Processing drama: {drama_name}")
        playlist = Playlist(data['link'])
        playlist._video_regex = re.compile(r'"url":"(/watch\?v=[\w-]*)')
        
        print(f"🔍 Found {len(playlist.video_urls)} videos")
        
        for idx, url in enumerate(playlist.video_urls, 1):
            print(f"\n📼 Episode {idx}: {url}")
            
            # Duration check
            duration = get_video_duration(url)
            print(f"⏱️  Duration: {duration//60}m {duration%60}s")
            
            if duration < MIN_DURATION:
                print("⏭️  Skipping short video")
                continue
                
            # Get transcripts
            video_id = url_to_id(url)
            print(f"🔧 Video ID: {video_id}")
            
            en_transcript, ur_transcript = None, None
            for attempt in range(RETRY_ATTEMPTS):
                try:
                    en_transcript, ur_transcript = get_transcripts(video_id)
                    break
                except Exception as e:
                    print(f"Attempt {attempt+1} failed: {str(e)}")
                    sleep(REQUEST_DELAY)

            # Save files
            base_path = f"transcripts/{drama_name}_Ep_{idx}"
            if en_transcript:
                save_transcript(en_transcript, f"{base_path}_English_T.txt")
                save_transcript(en_transcript, f"{base_path}_English.txt", False)
            if ur_transcript:
                save_transcript(ur_transcript, f"{base_path}_Urdu_T.txt")
                save_transcript(ur_transcript, f"{base_path}_Urdu.txt", False)
                
            print("✅ Success!" if en_transcript or ur_transcript else "⏭️  No transcripts")

def url_to_id(url):
    """Extract video ID from URL"""
    patterns = [
        r"v=([\w-]{11})",
        r"be/([\w-]{11})",
        r"embed/([\w-]{11})",
        r"/([\w-]{11})$"
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return url.split('/')[-1]

def save_transcript(transcript, filename, with_timestamps=True):
    """Save transcript to file"""
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w', encoding='utf-8') as f:
        if with_timestamps:
            for entry in transcript:
                f.write(f"[{entry['start']:.2f}] {entry['text']}\n")
        else:
            f.write(' '.join([entry['text'] for entry in transcript]))

if __name__ == "__main__":
    process_dramas()
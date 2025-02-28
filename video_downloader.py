import os
import re
import time
import json
import threading
import logging
import requests
import subprocess
from pytube import Playlist
import concurrent.futures

# Configuration
MAX_RETRY_ATTEMPTS = 5
REQUEST_DELAY = 2
DOWNLOAD_DIR = "downloads"
TRANSCRIPT_DIR = "transcripts"
MAX_THREADS = 4
INSTANCE_ID = os.environ.get("AWS_INSTANCE_ID", f"worker-{threading.get_native_id()}")

# Terabox credentials - Replace with your actual credentials
TERABOX_USERNAME = "2022cs620@student.uet.edu.pk"
TERABOX_PASSWORD = "Usm1230@"

# Import drama data from transcript_fetcher
try:
    from transcript_fetcher import dramas, url_to_id
except ImportError:
    print("ERROR: Failed to import data from transcript_fetcher.py")
    raise

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("video_downloader.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("video_downloader")

class TeraboxUploader:
    def __init__(self):
        """Initialize Terabox API client"""
        self.session = requests.Session()
        self.logged_in = False
        self.base_url = "https://www.terabox.com/api"
        
    def login(self):
        """Login to Terabox account"""
        print("Terabox functionality is disabled for now - storing files locally only")
        return False

class VideoDownloader:
    def __init__(self):
        print("\n" + "*"*60)
        print(f"DRAMA VIDEO DOWNLOADER (Version 1.1)")
        print(f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Running on instance: {INSTANCE_ID}")
        print("*"*60 + "\n")
        
        # Check for yt-dlp
        self.yt_dlp_available = self._check_yt_dlp()
        if not self.yt_dlp_available:
            print("\n⚠️ yt-dlp not found. Please install it:")
            print("pip install yt-dlp")
            print("or download from: https://github.com/yt-dlp/yt-dlp/releases")
            print("Continuing with limited functionality...\n")
        
        self.lock = threading.Lock()
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS)
        self.processed_episodes = set()
        
        # Initialize Terabox uploader
        self.terabox = TeraboxUploader()
        self.terabox_available = False
        print("⚠ Terabox integration disabled. Running in LOCAL MODE (files will be saved locally only)")
    
    def _check_yt_dlp(self):
        """Check if yt-dlp is installed"""
        try:
            result = subprocess.run(
                ["yt-dlp", "--version"], 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                text=True
            )
            if result.returncode == 0:
                print(f"Found yt-dlp version: {result.stdout.strip()}")
                return True
            return False
        except FileNotFoundError:
            return False
    
    def download_video(self, url, output_path):
        """Download a YouTube video using yt-dlp"""
        print(f"Starting download from URL: {url}")
        print(f"Output path: {output_path}")
        
        # Make sure the output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        if self.yt_dlp_available:
            return self._download_with_yt_dlp(url, output_path)
        else:
            print("Attempting direct download with requests (limited functionality)")
            return self._download_with_requests(url, output_path)
    
    def _download_with_yt_dlp(self, url, output_path):
        """Download video using yt-dlp"""
        for attempt in range(MAX_RETRY_ATTEMPTS):
            try:
                print(f"Download attempt {attempt+1}/{MAX_RETRY_ATTEMPTS} using yt-dlp...")
                
                # Build the yt-dlp command
                cmd = [
                    "yt-dlp",
                    url,
                    "-o", output_path,
                    "--no-playlist",
                    "--format", "best[ext=mp4]",
                    "--no-check-certificate",
                    "--no-warnings"
                ]
                
                print(f"Running command: {' '.join(cmd)}")
                
                # Run yt-dlp
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                
                # Process output in real-time
                while True:
                    output = process.stdout.readline()
                    if output == '' and process.poll() is not None:
                        break
                    if output:
                        print(output.strip())
                
                # Get the return code
                return_code = process.poll()
                
                # Check if download succeeded
                if return_code == 0 and os.path.exists(output_path):
                    file_size = os.path.getsize(output_path) / (1024 * 1024)
                    print(f"Download complete! File saved to {output_path} ({file_size:.2f} MB)")
                    return True
                else:
                    stderr = process.stderr.read()
                    print(f"yt-dlp error: {stderr}")
                    
            except Exception as e:
                logger.error(f"Download attempt {attempt+1} failed: {str(e)}")
                print(f"Download attempt {attempt+1} failed with error: {str(e)}")
            
            # Only retry if we haven't succeeded
            print(f"Waiting {REQUEST_DELAY * (attempt + 1)} seconds before retrying...")
            time.sleep(REQUEST_DELAY * (attempt + 1))
        
        print("All download attempts failed")
        return False
    
    def _download_with_requests(self, url, output_path):
        """Very basic direct download attempt (fallback only)"""
        print("⚠️ Attempting direct download (not recommended)...")
        print("This method will likely fail with YouTube videos.")
        print("Please install yt-dlp for better results.")
        
        try:
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                with open(output_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=1024*1024):
                        if chunk:
                            f.write(chunk)
                if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                    print(f"Download complete! File saved to {output_path}")
                    return True
                else:
                    print("Download failed - empty file received")
            else:
                print(f"Direct download failed with status code: {response.status_code}")
        except Exception as e:
            print(f"Direct download failed: {str(e)}")
        
        return False
    
    def process_episode(self, drama_name, idx, url):
        """Process a single episode"""
        # Check if this job is already processed
        episode_key = f"{drama_name}_{idx}"
        if episode_key in self.processed_episodes:
            print(f"Skipping {drama_name} episode {idx} - already processed in this session")
            return False
            
        episode_filename = f"{drama_name}_Ep_{idx}.mp4"
        local_path = os.path.join(DOWNLOAD_DIR, drama_name, episode_filename)
        
        # Create local directory if it doesn't exist
        if not os.path.exists(os.path.dirname(local_path)):
            print(f"Creating directory: {os.path.dirname(local_path)}")
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        logger.info(f"Processing {drama_name} episode {idx}: {url}")
        print(f"\n--------- PROCESSING {drama_name} Episode {idx} ---------")
        print(f"YouTube URL: {url}")
        print(f"Local path: {local_path}")
        
        # Download the video
        print("\n--- VIDEO DOWNLOAD PHASE ---")
        if self.download_video(url, local_path):
            logger.info(f"Successfully downloaded {episode_filename}")
            print(f"✓ Downloaded: {episode_filename}")
            
            # Check for corresponding transcripts
            print("\n--- TRANSCRIPT PROCESSING PHASE ---")
            transcript_base = f"transcripts/{drama_name}_Ep_{idx}"
            transcript_files = [
                f"{transcript_base}_English_T.txt",
                f"{transcript_base}_English.txt",
                f"{transcript_base}_Urdu_T.txt",
                f"{transcript_base}_Urdu.txt"
            ]
            
            print(f"Checking for transcript files with base: {transcript_base}")
            
            # Check for transcripts
            transcript_count = 0
            for transcript_file in transcript_files:
                if os.path.exists(transcript_file):
                    print(f"Found transcript: {transcript_file}")
                    transcript_count += 1
                else:
                    print(f"Transcript not found: {transcript_file}")
            
            if transcript_count == 0:
                print("No transcript files found")
            else:
                print(f"✓ Found {transcript_count} transcript files")
            
            # Mark episode as processed
            self.processed_episodes.add(episode_key)
            print(f"✓ Marked episode as processed")
            print(f"--------- FINISHED {drama_name} Episode {idx} ---------\n")
            return True
        else:
            logger.error(f"Failed to download episode {idx}")
            print(f"✗ Failed to download episode {idx}")
        
        return False
    
    def process_drama_sequentially(self, drama_name):
        """Process a single drama with episodes in sequence"""
        print(f"\n\n========== STARTING DRAMA: {drama_name} ==========")
        logger.info(f"Processing drama: {drama_name}")
        
        print(f"Getting playlist information for {drama_name}...")
        data = dramas[drama_name]
        print(f"Playlist URL: {data['link']}")
        
        try:
            playlist = Playlist(data['link'])
            playlist._video_regex = re.compile(r'"url":"(/watch\?v=[\w-]*)')
            
            # Get video URLs
            video_urls = []
            try:
                video_urls = list(playlist.video_urls)
                total_episodes = len(video_urls)
                logger.info(f"Found {total_episodes} videos in playlist")
                print(f"Found {total_episodes} episodes in drama playlist")
            except Exception as e:
                logger.error(f"Failed to get playlist videos: {str(e)}")
                print(f"Error getting playlist videos: {str(e)}")
                
                # Fallback: try to extract URLs using yt-dlp
                if self.yt_dlp_available:
                    print("Attempting to get playlist info with yt-dlp...")
                    try:
                        cmd = ["yt-dlp", "--flat-playlist", "--get-id", data['link']]
                        result = subprocess.run(cmd, capture_output=True, text=True)
                        if result.returncode == 0:
                            video_ids = result.stdout.strip().split("\n")
                            video_urls = [f"https://www.youtube.com/watch?v={vid}" for vid in video_ids if vid]
                            total_episodes = len(video_urls)
                            print(f"Found {total_episodes} episodes using yt-dlp")
                        else:
                            print(f"yt-dlp playlist extraction failed: {result.stderr}")
                    except Exception as e2:
                        print(f"yt-dlp playlist extraction error: {str(e2)}")
            
            if not video_urls:
                print("No videos found in playlist. Aborting drama processing.")
                return
            
            # Process episodes one by one in sequence
            successful_episodes = 0
            for idx, url in enumerate(video_urls, 1):
                print(f"\n{'='*50}")
                print(f"PROCESSING EPISODE {idx}/{total_episodes}")
                print(f"{'='*50}")
                if self.process_episode(drama_name, idx, url):
                    successful_episodes += 1
                
                # Short delay between episodes
                delay = REQUEST_DELAY
                print(f"Waiting {delay} seconds before next episode...")
                time.sleep(delay)
            
            print(f"\n========== COMPLETED DRAMA: {drama_name} ==========")
            print(f"Successfully processed {successful_episodes} out of {total_episodes} episodes\n\n")
            logger.info(f"Completed drama {drama_name}: {successful_episodes}/{total_episodes} episodes")
            
        except Exception as e:
            print(f"ERROR processing drama {drama_name}: {str(e)}")
            logger.error(f"Drama processing error: {str(e)}")
    
    def process_all_dramas(self):
        """Process all dramas one by one sequentially"""
        logger.info("Starting video download process for all dramas")
        print("\n" + "="*50)
        print("===== DRAMA DOWNLOAD PROCESS STARTED =====")
        print("="*50)
        
        total_dramas = len(dramas)
        print(f"Found {total_dramas} dramas to process:")
        for i, drama_name in enumerate(dramas, 1):
            print(f"  {i}. {drama_name}")
        
        # Process each drama sequentially
        completed_dramas = 0
        for idx, drama_name in enumerate(dramas, 1):
            print(f"\n\n{'#'*60}")
            print(f"### DRAMA {idx}/{total_dramas}: {drama_name}")
            print(f"{'#'*60}")
            
            try:
                self.process_drama_sequentially(drama_name)
                completed_dramas += 1
            except Exception as e:
                print(f"Error processing drama {drama_name}: {str(e)}")
                logger.error(f"Fatal error in drama {drama_name}: {str(e)}")
        
        print("\n" + "="*50)
        print("===== DRAMA DOWNLOAD PROCESS COMPLETED =====")
        print(f"Successfully processed {completed_dramas}/{total_dramas} dramas")
        print("="*50)
        logger.info(f"Completed processing all dramas: {completed_dramas}/{total_dramas}")


if __name__ == "__main__":
    # Create directories if they don't exist
    if not os.path.exists(DOWNLOAD_DIR):
        print(f"Creating download directory: {DOWNLOAD_DIR}")
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    print("\nInitializing downloader...")
    downloader = VideoDownloader()
    
    print("\nStarting drama processing...")
    downloader.process_all_dramas()
    
    print(f"\nScript completed at: {time.strftime('%Y-%m-%d %H:%M:%S')}") 
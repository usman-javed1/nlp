import os
import re
import time
import json
import threading
import logging
from pytube import YouTube, Playlist
import concurrent.futures

# Try to import boto3, but continue without it if not available
try:
    import boto3
    from botocore.exceptions import ClientError
    boto3_available = True
except ImportError:
    boto3_available = False
    print("WARNING: boto3 not available. S3 functionality will be disabled.")

# Try to load environment variables, but continue if not available
try:
    from dotenv import load_dotenv
    load_dotenv()
    env_loaded = True
except ImportError:
    env_loaded = False
    print("WARNING: python-dotenv not available. Using default configuration.")

# Configuration
MAX_RETRY_ATTEMPTS = 5
REQUEST_DELAY = 2
DOWNLOAD_DIR = "downloads"
TRANSCRIPT_DIR = "transcripts"
MAX_THREADS = 4
INSTANCE_ID = os.environ.get("AWS_INSTANCE_ID", f"worker-{threading.get_native_id()}")

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

class VideoDownloader:
    def __init__(self):
        print("\n" + "*"*60)
        print(f"DRAMA VIDEO DOWNLOADER (Version 1.0)")
        print(f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Running on instance: {INSTANCE_ID}")
        print("*"*60 + "\n")
        
        # Initialize S3 client (if available)
        self.s3_available = False
        if boto3_available:
            try:
                # Try to create S3 client
                self.s3_client = boto3.client('s3')
                
                # Test connection
                response = self.s3_client.list_buckets()
                print(f"✓ AWS credentials valid. Found {len(response['Buckets'])} buckets.")
                self.s3_available = True
            except Exception as e:
                print(f"⚠ S3 will not be used: {str(e)}")
                print("Running in LOCAL MODE (files will be saved locally only)")
        else:
            print("⚠ boto3 not available. Running in LOCAL MODE.")
        
        self.lock = threading.Lock()
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS)
        self.processed_episodes = set()
        
    def acquire_job(self, drama_name, episode_idx):
        """Check if job is available (always returns True in local mode)"""
        episode_key = f"{drama_name}_{episode_idx}"
        
        # If already processed in this session, skip
        if episode_key in self.processed_episodes:
            print(f"Already processed {drama_name} Episode {episode_idx} locally in this session")
            return False
        
        # If S3 is not available, assume job is available
        if not self.s3_available:
            print(f"Running in local mode. Processing: {drama_name} Episode {episode_idx}")
            return True
        
        # Rest of S3 coordination code (this won't be executed in local mode)
        return True
    
    def mark_job_complete(self, drama_name, episode_idx):
        """Mark a job as completed (locally in memory)"""
        episode_key = f"{drama_name}_{episode_idx}"
        self.processed_episodes.add(episode_key)
        
        if not self.s3_available:
            print(f"Marked complete locally: {drama_name} Episode {episode_idx}")
            return True
        
        # Rest of S3 code (won't be executed)
        return True
    
    def download_video(self, url, output_path):
        """Download a YouTube video using pytube"""
        print(f"Starting download from URL: {url}")
        print(f"Output path: {output_path}")
        
        for attempt in range(MAX_RETRY_ATTEMPTS):
            try:
                print(f"Download attempt {attempt+1}/{MAX_RETRY_ATTEMPTS}...")
                yt = YouTube(url)
                print(f"YouTube object created. Video title: {yt.title}")
                
                # Get highest resolution stream with video and audio
                print("Searching for best quality stream...")
                stream = yt.streams.filter(progressive=True).order_by('resolution').desc().first()
                
                if not stream:
                    print("No progressive stream found, falling back to highest resolution")
                    stream = yt.streams.filter(file_extension='mp4').order_by('resolution').desc().first()
                    
                if stream:
                    print(f"Found stream: {stream.resolution}, {stream.mime_type}, {stream.filesize_mb:.2f} MB")
                    print(f"Downloading file...")
                    stream.download(filename=output_path)
                    print(f"Download complete! File saved to {output_path}")
                    return True
                else:
                    print("No suitable stream found for this video")
                    logger.error("No suitable stream found")
                    return False
                    
            except Exception as e:
                logger.error(f"Download attempt {attempt+1} failed: {str(e)}")
                print(f"Download attempt {attempt+1} failed with error: {str(e)}")
                print(f"Waiting {REQUEST_DELAY * (attempt + 1)} seconds before retrying...")
                time.sleep(REQUEST_DELAY * (attempt + 1))
        
        print("All download attempts failed")
        return False
    
    def process_episode(self, drama_name, idx, url):
        """Process a single episode"""
        # Check if this job is already taken or completed
        print(f"\nChecking if {drama_name} Episode {idx} is available to process...")
        if not self.acquire_job(drama_name, idx):
            logger.info(f"Skipping {drama_name} episode {idx} - already processed or being processed")
            print(f"Skipping {drama_name} episode {idx} - already processed or being processed")
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
            
            # Mark job as complete
            self.mark_job_complete(drama_name, idx)
            print(f"✓ Marked job as complete")
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
            
            total_episodes = len(playlist.video_urls)
            logger.info(f"Found {total_episodes} videos in playlist")
            print(f"Found {total_episodes} episodes in drama playlist")
            
            # Process episodes one by one in sequence
            successful_episodes = 0
            for idx, url in enumerate(playlist.video_urls, 1):
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
    print("\n" + "*"*60)
    print(f"DRAMA VIDEO DOWNLOADER (Version 1.0)")
    print(f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Running on instance: {INSTANCE_ID}")
    print("*"*60 + "\n")
    
    # Create directories if they don't exist
    if not os.path.exists(DOWNLOAD_DIR):
        print(f"Creating download directory: {DOWNLOAD_DIR}")
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    print("\nInitializing downloader...")
    downloader = VideoDownloader()
    
    print("\nStarting drama processing...")
    downloader.process_all_dramas()
    
    print(f"\nScript completed at: {time.strftime('%Y-%m-%d %H:%M:%S')}") 
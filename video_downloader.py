import os
import re
import time
import json
import threading
import logging
import requests
import subprocess
import tempfile
from pytube import Playlist
import concurrent.futures

# Configuration
MAX_RETRY_ATTEMPTS = 5
REQUEST_DELAY = 2
TEMP_DIR = tempfile.gettempdir()  # Use system temp directory
TRANSCRIPT_DIR = "transcripts"
MAX_THREADS = 4
INSTANCE_ID = os.environ.get("AWS_INSTANCE_ID", f"worker-{threading.get_native_id()}")

# Terabox credentials - Replace with your actual credentials
TERABOX_USERNAME = "your_terabox_email"
TERABOX_PASSWORD = "your_terabox_password"

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
        # Try to login immediately
        self.login()
        
    def login(self):
        """Login to Terabox account"""
        print("Attempting to login to Terabox...")
        try:
            login_url = f"{self.base_url}/login"
            payload = {
                "username": TERABOX_USERNAME,
                "password": TERABOX_PASSWORD
            }
            
            response = self.session.post(login_url, data=payload)
            if response.status_code == 200:
                data = response.json()
                if data.get("errno") == 0:
                    self.logged_in = True
                    print("✓ Successfully logged in to Terabox")
                    return True
                else:
                    print(f"✗ Terabox login failed: {data.get('errmsg', 'Unknown error')}")
            else:
                print(f"✗ Terabox login failed with status code: {response.status_code}")
                
            return False
        except Exception as e:
            print(f"✗ Terabox login error: {str(e)}")
            return False
    
    def create_folder(self, folder_path):
        """Create a folder on Terabox (if it doesn't exist)"""
        if not self.logged_in and not self.login():
            print("Cannot create folder: Not logged in to Terabox")
            return False
            
        try:
            print(f"Creating folder in Terabox: {folder_path}")
            create_url = f"{self.base_url}/create"
            payload = {
                "path": folder_path,
                "isdir": 1
            }
            
            response = self.session.post(create_url, data=payload)
            if response.status_code == 200:
                data = response.json()
                if data.get("errno") == 0 or data.get("errno") == 31061:  # 31061 means folder already exists
                    print(f"✓ Folder ready: {folder_path}")
                    return True
                else:
                    print(f"✗ Failed to create folder: {data.get('errmsg', 'Unknown error')}")
            else:
                print(f"✗ Failed to create folder with status code: {response.status_code}")
                
            return False
        except Exception as e:
            print(f"✗ Create folder error: {str(e)}")
            return False
    
    def upload_file(self, local_path, remote_path):
        """Upload a file to Terabox"""
        if not self.logged_in and not self.login():
            print("Cannot upload file: Not logged in to Terabox")
            return None
            
        try:
            print(f"Uploading file to Terabox: {local_path} → {remote_path}")
            file_size = os.path.getsize(local_path) / (1024 * 1024)
            print(f"File size: {file_size:.2f} MB")
            
            # Ensure parent directory exists
            parent_dir = os.path.dirname(remote_path)
            if parent_dir and not self.create_folder(parent_dir):
                print(f"Failed to create parent directory: {parent_dir}")
                return None
            
            # Upload the file
            upload_url = f"{self.base_url}/upload"
            with open(local_path, 'rb') as file:
                files = {'file': (os.path.basename(local_path), file)}
                payload = {'path': remote_path}
                
                print("Starting upload...")
                response = self.session.post(upload_url, data=payload, files=files)
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get("errno") == 0:
                        print(f"✓ Successfully uploaded file to Terabox")
                        # Get the share link
                        file_id = data.get("fs_id")
                        share_link = self.get_share_link(file_id) if file_id else None
                        return share_link
                    else:
                        print(f"✗ Upload failed: {data.get('errmsg', 'Unknown error')}")
                else:
                    print(f"✗ Upload failed with status code: {response.status_code}")
                
            return None
        except Exception as e:
            print(f"✗ Upload error: {str(e)}")
            return None
    
    def get_share_link(self, file_id):
        """Get a shareable link for the uploaded file"""
        try:
            print(f"Getting share link for file ID: {file_id}")
            share_url = f"{self.base_url}/share/set"
            payload = {
                "fid_list": f"[{file_id}]",
                "period": 0,  # Permanent share
                "channel_list": "[]",
                "pwd": ""  # No password
            }
            
            response = self.session.post(share_url, data=payload)
            if response.status_code == 200:
                data = response.json()
                if data.get("errno") == 0:
                    share_info = data.get("shorturl")
                    if share_info:
                        print(f"✓ Generated share link: {share_info}")
                        return share_info
                    else:
                        print("✗ Share link not found in response")
                else:
                    print(f"✗ Failed to generate share link: {data.get('errmsg', 'Unknown error')}")
            else:
                print(f"✗ Share link request failed with status code: {response.status_code}")
                
            return None
        except Exception as e:
            print(f"✗ Share link error: {str(e)}")
            return None

class VideoDownloader:
    def __init__(self):
        print("\n" + "*"*60)
        print(f"DRAMA VIDEO DOWNLOADER (Version 1.2)")
        print(f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Running on instance: {INSTANCE_ID}")
        print(f"Temp directory: {TEMP_DIR}")
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
        print("Initializing Terabox uploader...")
        self.terabox = TeraboxUploader()
        self.terabox_available = self.terabox.logged_in
        
        if self.terabox_available:
            print("✓ Terabox login successful. Will upload files directly to Terabox.")
        else:
            print("⚠ Terabox login failed. Cannot upload files!")
            raise Exception("Terabox login failed. Aborting as direct upload was requested.")
    
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
        print(f"Temporary output path: {output_path}")
        
        # Make sure the output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        if self.yt_dlp_available:
            return self._download_with_yt_dlp(url, output_path)
        else:
            print("yt-dlp is required for direct Terabox uploads. Please install it.")
            return False
    
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
    
    def process_episode(self, drama_name, idx, url):
        """Process a single episode"""
        # Check if this job is already processed
        episode_key = f"{drama_name}_{idx}"
        if episode_key in self.processed_episodes:
            print(f"Skipping {drama_name} episode {idx} - already processed in this session")
            return False
            
        episode_filename = f"{drama_name}_Ep_{idx}.mp4"
        temp_path = os.path.join(TEMP_DIR, episode_filename)
        terabox_path = f"/dramas/{drama_name}/{episode_filename}"
        
        logger.info(f"Processing {drama_name} episode {idx}: {url}")
        print(f"\n--------- PROCESSING {drama_name} Episode {idx} ---------")
        print(f"YouTube URL: {url}")
        print(f"Temporary path: {temp_path}")
        print(f"Terabox destination: {terabox_path}")
        
        # Download the video
        print("\n--- VIDEO DOWNLOAD PHASE ---")
        download_success = self.download_video(url, temp_path)
        
        if download_success:
            logger.info(f"Successfully downloaded {episode_filename}")
            print(f"✓ Downloaded: {episode_filename}")
            
            # Upload to Terabox
            print("\n--- TERABOX UPLOAD PHASE ---")
            terabox_link = self.terabox.upload_file(temp_path, terabox_path)
            
            # Delete temporary file regardless of upload success
            try:
                print(f"Deleting temporary file: {temp_path}")
                os.remove(temp_path)
                print(f"✓ Cleaned up temporary file")
            except Exception as e:
                print(f"⚠ Failed to delete temporary file: {str(e)}")
            
            if terabox_link:
                print(f"✓ Uploaded to Terabox: {terabox_path}")
                print(f"✓ Terabox Link: {terabox_link}")
                
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
                
                # Upload transcripts if they exist
                transcript_count = 0
                for transcript_file in transcript_files:
                    if os.path.exists(transcript_file):
                        print(f"Found transcript: {transcript_file}")
                        
                        # Upload transcript to Terabox
                        terabox_transcript_path = f"/transcripts/{drama_name}/{os.path.basename(transcript_file)}"
                        transcript_link = self.terabox.upload_file(transcript_file, terabox_transcript_path)
                        if transcript_link:
                            print(f"✓ Uploaded transcript to Terabox: {transcript_link}")
                        transcript_count += 1
                    else:
                        print(f"Transcript not found: {transcript_file}")
                
                if transcript_count == 0:
                    print("No transcript files found")
                else:
                    print(f"✓ Processed {transcript_count} transcript files")
                
                # Mark as processed only if video upload succeeded
                self.processed_episodes.add(episode_key)
                print(f"✓ Marked episode as processed")
                print(f"--------- FINISHED {drama_name} Episode {idx} ---------\n")
                return True
            else:
                logger.error(f"Failed to upload {episode_filename} to Terabox")
                print(f"✗ Failed to upload {episode_filename} to Terabox")
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
    print("\nInitializing downloader...")
    downloader = VideoDownloader()
    
    print("\nStarting drama processing...")
    downloader.process_all_dramas()
    
    print(f"\nScript completed at: {time.strftime('%Y-%m-%d %H:%M:%S')}") 
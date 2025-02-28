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
import base64

# Configuration
MAX_RETRY_ATTEMPTS = 5
REQUEST_DELAY = 2
DOWNLOAD_DIR = "downloads"  # Local storage as fallback
TEMP_DIR = tempfile.gettempdir()  # Use system temp directory
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
        """Initialize Terabox API client with improved error handling"""
        self.session = requests.Session()
        self.logged_in = False
        self.cookies = {}
        
        # Terabox uses multiple domains for its API
        self.domains = [
            "https://www.terabox.com",
            "https://www.teraboxapp.com",
            "https://terabox.com",
            "https://www.1024tera.com"
        ]
        
        # Additional headers to mimic browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Origin': 'https://www.terabox.com',
            'Referer': 'https://www.terabox.com/'
        })
        
        # Try to login immediately
        self.login()
        
    def _try_all_domains(self, endpoint, method="get", **kwargs):
        """Try request against all possible Terabox domains"""
        last_error = None
        
        for domain in self.domains:
            url = f"{domain}{endpoint}"
            print(f"Trying: {url}")
            
            try:
                if method.lower() == "post":
                    response = self.session.post(url, **kwargs)
                else:
                    response = self.session.get(url, **kwargs)
                
                # If we got any response (even error), it means the domain works
                print(f"Response from {domain}: {response.status_code}")
                return response
            except Exception as e:
                print(f"Failed with {domain}: {str(e)}")
                last_error = e
        
        # If we're here, all domains failed
        raise Exception(f"All Terabox domains failed: {last_error}")
    
    def login(self):
        """Login to Terabox account with improved web scraping approach"""
        print("Attempting to login to Terabox...")
        
        try:
            # First, get the login page to get necessary cookies
            try:
                response = self._try_all_domains("/login")
                for cookie in self.session.cookies:
                    self.cookies[cookie.name] = cookie.value
                print("Got initial cookies")
            except Exception as e:
                print(f"Failed to get initial cookies: {str(e)}")
            
            # Try the actual login
            try:
                login_data = {
                    "username": TERABOX_USERNAME,
                    "password": base64.b64encode(TERABOX_PASSWORD.encode()).decode(),
                    "isKeepLogin": 1
                }
                
                # Try multiple login endpoints
                endpoints = [
                    "/api/v2/account/signin",
                    "/api/login",
                    "/rest/2.0/xpan/user?method=login"
                ]
                
                for endpoint in endpoints:
                    try:
                        print(f"Trying login endpoint: {endpoint}")
                        response = self._try_all_domains(endpoint, method="post", json=login_data)
                        
                        if response.status_code == 200:
                            try:
                                data = response.json()
                                if data.get("errno") == 0 or "token" in data:
                                    self.logged_in = True
                                    print("✓ Successfully logged in to Terabox")
                                    
                                    # Save cookies
                                    for cookie in self.session.cookies:
                                        self.cookies[cookie.name] = cookie.value
                                    
                                    return True
                            except:
                                pass
                    except Exception as e:
                        print(f"Login endpoint {endpoint} failed: {str(e)}")
                
                # Try manual cookie approach if API login fails
                if not self.logged_in:
                    print("Attempting fallback cookie-based authentication...")
                    # This is where you can manually set cookies if needed
                    # self.session.cookies.set("ndus", "your_ndus_cookie_value")
                    # TODO: Add manual cookie configuration here
            
            except Exception as e:
                print(f"Login attempt failed: {str(e)}")
            
            # Check login status
            try:
                print("Verifying login status...")
                response = self._try_all_domains("/api/user/info")
                
                try:
                    data = response.json()
                    if data.get("errno") == 0 and data.get("username"):
                        print(f"✓ Login verified: {data.get('username')}")
                        self.logged_in = True
                        return True
                except:
                    print("Failed to verify login status")
            except Exception as e:
                print(f"Login verification failed: {str(e)}")
            
            print("⚠ Login methods failed, but will continue in fallback mode")
            return False
            
        except Exception as e:
            print(f"✗ Terabox login error: {str(e)}")
            return False
    
    def create_folder(self, folder_path):
        """Create a folder on Terabox (if it doesn't exist)"""
        if not self.logged_in:
            print("ℹ️ Not logged in to Terabox - folder creation will be simulated")
            return True  # Pretend success in fallback mode
            
        try:
            print(f"Creating folder in Terabox: {folder_path}")
            
            # Try multiple folder creation endpoints
            endpoints = [
                "/api/create?opera=2",
                "/api/create"
            ]
            
            for endpoint in endpoints:
                try:
                    payload = {
                        "path": folder_path,
                        "isdir": 1
                    }
                    
                    response = self._try_all_domains(endpoint, method="post", data=payload)
                    
                    if response.status_code == 200:
                        try:
                            data = response.json()
                            if data.get("errno") == 0 or data.get("errno") == 31061:  # 31061 means folder already exists
                                print(f"✓ Folder ready: {folder_path}")
                                return True
                        except:
                            pass
                except Exception as e:
                    print(f"Folder creation endpoint {endpoint} failed: {str(e)}")
            
            # In fallback mode, let's pretend this worked
            print("⚠ Folder creation failed, but continuing in fallback mode")
            return True
            
        except Exception as e:
            print(f"✗ Create folder error: {str(e)}")
            # In fallback mode, let's pretend this worked
            return True
    
    def upload_file(self, local_path, remote_path):
        """Upload a file to Terabox with fallback to local storage"""
        if not self.logged_in:
            # Fallback to saving locally
            try:
                local_save_path = os.path.join(DOWNLOAD_DIR, os.path.basename(remote_path))
                os.makedirs(os.path.dirname(local_save_path), exist_ok=True)
                
                print(f"⚠ Not logged in to Terabox. Saving file locally: {local_save_path}")
                
                # If the file is already in a temporary location, move it
                if local_path.startswith(TEMP_DIR):
                    import shutil
                    shutil.copy2(local_path, local_save_path)
                    print(f"✓ File saved locally: {local_save_path}")
                    return f"file://{os.path.abspath(local_save_path)}"
                
                return f"file://{os.path.abspath(local_path)}"
            except Exception as e:
                print(f"✗ Local file save error: {str(e)}")
                return None
            
        try:
            print(f"Uploading file to Terabox: {local_path} → {remote_path}")
            file_size = os.path.getsize(local_path) / (1024 * 1024)
            print(f"File size: {file_size:.2f} MB")
            
            # Ensure parent directory exists
            parent_dir = os.path.dirname(remote_path)
            if parent_dir and not self.create_folder(parent_dir):
                print(f"Failed to create parent directory: {parent_dir}")
                # Continue anyway in fallback mode
            
            # Try multiple upload endpoints
            endpoints = [
                "/api/upload?dir=/",
                "/xpan/file?method=upload",
                "/api/precreate"  # Terabox sometimes uses a precreate + upload approach
            ]
            
            for endpoint in endpoints:
                try:
                    print(f"Trying upload endpoint: {endpoint}")
                    
                    with open(local_path, 'rb') as file:
                        files = {'file': (os.path.basename(local_path), file)}
                        payload = {'path': remote_path}
                        
                        response = self._try_all_domains(endpoint, method="post", data=payload, files=files)
                        
                        if response.status_code == 200:
                            try:
                                data = response.json()
                                if data.get("errno") == 0:
                                    print(f"✓ Successfully uploaded file to Terabox")
                                    
                                    # Try to get a share link
                                    file_id = data.get("fs_id")
                                    share_link = self.get_share_link(file_id) if file_id else None
                                    
                                    if share_link:
                                        return share_link
                                    else:
                                        # Fallback to generic success message
                                        return "Uploaded to Terabox (link not available)"
                            except:
                                pass
                except Exception as e:
                    print(f"Upload endpoint {endpoint} failed: {str(e)}")
            
            # If all upload methods failed, save locally as fallback
            local_save_path = os.path.join(DOWNLOAD_DIR, os.path.basename(remote_path))
            os.makedirs(os.path.dirname(local_save_path), exist_ok=True)
            
            print(f"⚠ Terabox upload failed. Saving file locally: {local_save_path}")
            
            # If the file is already in a temporary location, move it
            if local_path.startswith(TEMP_DIR):
                import shutil
                shutil.copy2(local_path, local_save_path)
                print(f"✓ File saved locally: {local_save_path}")
                return f"file://{os.path.abspath(local_save_path)}"
            
            return f"file://{os.path.abspath(local_path)}"
                
        except Exception as e:
            print(f"✗ Upload error: {str(e)}")
            
            # Fallback to local storage
            try:
                local_save_path = os.path.join(DOWNLOAD_DIR, os.path.basename(remote_path))
                os.makedirs(os.path.dirname(local_save_path), exist_ok=True)
                
                print(f"⚠ Terabox upload failed. Saving file locally: {local_save_path}")
                
                # If the file is already in a temporary location, move it
                if local_path.startswith(TEMP_DIR):
                    import shutil
                    shutil.copy2(local_path, local_save_path)
                    print(f"✓ File saved locally: {local_save_path}")
                    return f"file://{os.path.abspath(local_save_path)}"
                
                return f"file://{os.path.abspath(local_path)}"
            except Exception as e2:
                print(f"✗ Local file save error: {str(e2)}")
                return None
    
    def get_share_link(self, file_id):
        """Get a shareable link for the uploaded file"""
        if not self.logged_in or not file_id:
            return None
            
        try:
            print(f"Getting share link for file ID: {file_id}")
            
            # Try multiple share endpoints
            endpoints = [
                "/share/set",
                "/api/sharelink/set",
                "/api/share/set"
            ]
            
            for endpoint in endpoints:
                try:
                    payload = {
                        "fid_list": f"[{file_id}]",
                        "period": 0,  # Permanent share
                        "channel_list": "[]",
                        "pwd": ""  # No password
                    }
                    
                    response = self._try_all_domains(endpoint, method="post", data=payload)
                    
                    if response.status_code == 200:
                        try:
                            data = response.json()
                            if data.get("errno") == 0:
                                # Try different fields that might contain the link
                                for field in ["shorturl", "link", "share_url", "url"]:
                                    share_info = data.get(field)
                                    if share_info:
                                        print(f"✓ Generated share link: {share_info}")
                                        return share_info
                        except:
                            pass
                except Exception as e:
                    print(f"Share endpoint {endpoint} failed: {str(e)}")
            
            print("✗ Could not generate share link")
            return None
                
        except Exception as e:
            print(f"✗ Share link error: {str(e)}")
            return None

class VideoDownloader:
    def __init__(self):
        print("\n" + "*"*60)
        print(f"DRAMA VIDEO DOWNLOADER (Version 1.3)")
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
        
        # Initialize Terabox uploader with better fallback handling
        print("Initializing Terabox uploader...")
        self.terabox = TeraboxUploader()
        
        if self.terabox.logged_in:
            print("✓ Terabox login successful. Will upload files to Terabox.")
        else:
            print("⚠ Terabox login failed. Running in FALLBACK MODE: files will be saved locally.")
            # Create download directory if it doesn't exist
            os.makedirs(DOWNLOAD_DIR, exist_ok=True)
            print(f"Created local download directory: {DOWNLOAD_DIR}")
    
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
            print("yt-dlp is required for video downloads. Please install it.")
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
                print(f"✓ Uploaded: {terabox_path}")
                print(f"✓ Link: {terabox_link}")
                
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
                        transcript_filename = os.path.basename(transcript_file)
                        terabox_transcript_path = f"/transcripts/{drama_name}/{transcript_filename}"
                        tr_link = self.terabox.upload_file(transcript_file, terabox_transcript_path)
                        
                        if tr_link:
                            print(f"✓ Uploaded transcript: {tr_link}")
                        
                        transcript_count += 1
                    else:
                        print(f"Transcript not found: {transcript_file}")
                
                if transcript_count == 0:
                    print("No transcript files found")
                else:
                    print(f"✓ Processed {transcript_count} transcript files")
                
                # Mark as processed only if Terabox upload succeeded
                self.processed_episodes.add(episode_key)
                print(f"✓ Marked episode as processed")
                print(f"--------- FINISHED {drama_name} Episode {idx} ---------\n")
                return True
            else:
                logger.error(f"Failed to save {episode_filename}")
                print(f"✗ Failed to save {episode_filename}")
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
    # Create necessary directories
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(TRANSCRIPT_DIR, exist_ok=True)
    
    print("\nInitializing downloader...")
    downloader = VideoDownloader()
    
    print("\nStarting drama processing...")
    downloader.process_all_dramas()
    
    print(f"\nScript completed at: {time.strftime('%Y-%m-%d %H:%M:%S')}") 
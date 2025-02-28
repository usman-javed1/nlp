import os
import re
import time
import json
import boto3
import requests
import threading
import logging
import concurrent.futures
from pytube import YouTube, Playlist
from botocore.exceptions import ClientError

# Configuration
MAX_RETRY_ATTEMPTS = 5
REQUEST_DELAY = 2
DOWNLOAD_DIR = "downloads"
TRANSCRIPT_DIR = "transcripts"
MAX_THREADS = 4  # Number of concurrent downloads
S3_BUCKET = "drama-content"  # S3 bucket for storing content
S3_COORD_BUCKET = "drama-coordination"  # S3 bucket for coordination
S3_PATH_PREFIX = "job_status/"

# Get instance ID from environment or use public IP
try:
    public_ip = requests.get('http://checkip.amazonaws.com', timeout=5).text.strip()
    INSTANCE_ID = os.environ.get("AWS_INSTANCE_ID", f"worker-{public_ip}")
except:
    INSTANCE_ID = os.environ.get("AWS_INSTANCE_ID", f"worker-{threading.get_native_id()}")

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

# Import drama data from transcript_fetcher
try:
    from transcript_fetcher import dramas, url_to_id
except ImportError:
    logger.error("Failed to import data from transcript_fetcher.py")
    raise

class VideoDownloader:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.lock = threading.Lock()
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS)
        
    def acquire_job(self, drama_name, episode_idx):
        """Try to claim a job using S3 as coordination mechanism"""
        job_key = f"{S3_PATH_PREFIX}{drama_name}/episode_{episode_idx}.json"
        job_data = {
            "status": "processing",
            "instance": INSTANCE_ID,
            "start_time": time.time(),
            "drama": drama_name,
            "episode": episode_idx
        }
        
        try:
            # Try to check if someone else is processing this job
            try:
                response = self.s3_client.get_object(Bucket=S3_COORD_BUCKET, Key=job_key)
                existing_job = json.loads(response['Body'].read().decode('utf-8'))
                
                # If job is complete or less than 1 hour old, don't take it
                if existing_job["status"] == "complete":
                    return False
                
                # If job is processing but older than 1 hour, assume it failed
                if time.time() - existing_job["start_time"] < 3600:
                    return False
                    
            except ClientError as e:
                if e.response['Error']['Code'] != 'NoSuchKey':
                    logger.error(f"S3 error: {str(e)}")
                    return False
            
            # Claim the job
            self.s3_client.put_object(
                Bucket=S3_COORD_BUCKET, 
                Key=job_key,
                Body=json.dumps(job_data).encode('utf-8')
            )
            return True
            
        except Exception as e:
            logger.error(f"Failed to acquire job: {str(e)}")
            return False
    
    def mark_job_complete(self, drama_name, episode_idx):
        """Mark a job as completed in S3"""
        job_key = f"{S3_PATH_PREFIX}{drama_name}/episode_{episode_idx}.json"
        job_data = {
            "status": "complete",
            "instance": INSTANCE_ID,
            "completion_time": time.time(),
            "drama": drama_name,
            "episode": episode_idx
        }
        
        try:
            self.s3_client.put_object(
                Bucket=S3_COORD_BUCKET, 
                Key=job_key,
                Body=json.dumps(job_data).encode('utf-8')
            )
            return True
        except Exception as e:
            logger.error(f"Failed to mark job complete: {str(e)}")
            return False
    
    def download_video(self, url, output_path):
        """Download a YouTube video using pytube"""
        for attempt in range(MAX_RETRY_ATTEMPTS):
            try:
                yt = YouTube(url)
                # Get highest resolution stream with video and audio
                stream = yt.streams.filter(progressive=True).order_by('resolution').desc().first()
                
                if not stream:
                    # Fallback to highest resolution video and merge with audio
                    logger.info("No progressive stream found, using highest resolution")
                    stream = yt.streams.filter(file_extension='mp4').order_by('resolution').desc().first()
                    
                if stream:
                    logger.info(f"Downloading {stream.resolution} video")
                    stream.download(filename=output_path)
                    return True
                else:
                    logger.error("No suitable stream found")
                    return False
                    
            except Exception as e:
                logger.error(f"Download attempt {attempt+1} failed: {str(e)}")
                time.sleep(REQUEST_DELAY * (attempt + 1))
        
        return False
    
    def upload_to_s3(self, local_path, s3_key):
        """Upload file to S3 bucket"""
        try:
            self.s3_client.upload_file(local_path, S3_BUCKET, s3_key)
            logger.info(f"Uploaded {local_path} to s3://{S3_BUCKET}/{s3_key}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload to S3: {str(e)}")
            return False
    
    def process_episode(self, drama_name, idx, url):
        """Process a single episode"""
        # Check if this job is already taken or completed
        if not self.acquire_job(drama_name, idx):
            logger.info(f"Skipping {drama_name} episode {idx} - already processed or being processed")
            return
            
        episode_filename = f"{drama_name}_Ep_{idx}.mp4"
        local_path = os.path.join(DOWNLOAD_DIR, drama_name, episode_filename)
        s3_video_key = f"dramas/{drama_name}/{episode_filename}"
        
        # Create local directory if it doesn't exist
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        logger.info(f"Processing {drama_name} episode {idx}: {url}")
        
        # Download the video
        if self.download_video(url, local_path):
            logger.info(f"Successfully downloaded {episode_filename}")
            
            # Upload to S3
            if self.upload_to_s3(local_path, s3_video_key):
                logger.info(f"Successfully uploaded {episode_filename} to S3")
                
                # Check for corresponding transcripts
                transcript_base = f"transcripts/{drama_name}_Ep_{idx}"
                transcript_files = [
                    f"{transcript_base}_English_T.txt",
                    f"{transcript_base}_English.txt",
                    f"{transcript_base}_Urdu_T.txt",
                    f"{transcript_base}_Urdu.txt"
                ]
                
                # Upload transcripts if they exist
                for transcript_file in transcript_files:
                    if os.path.exists(transcript_file):
                        s3_transcript_key = f"transcripts/{drama_name}/{os.path.basename(transcript_file)}"
                        if self.upload_to_s3(transcript_file, s3_transcript_key):
                            logger.info(f"Uploaded transcript {transcript_file}")
                
                # Delete local file after successful upload
                try:
                    os.remove(local_path)
                    logger.info(f"Deleted local file {local_path}")
                except Exception as e:
                    logger.warning(f"Failed to delete local file: {str(e)}")
                
                # Mark job as complete
                self.mark_job_complete(drama_name, idx)
            else:
                logger.error(f"Failed to upload {episode_filename} to S3")
        else:
            logger.error(f"Failed to download episode {idx}")
    
    def process_drama(self, drama_name):
        """Process a single drama with multithreading"""
        logger.info(f"Processing drama: {drama_name}")
        
        data = dramas[drama_name]
        playlist = Playlist(data['link'])
        playlist._video_regex = re.compile(r'"url":"(/watch\?v=[\w-]*)')
        
        logger.info(f"Found {len(playlist.video_urls)} videos in playlist")
        
        # Submit each episode as a task to the thread pool
        futures = []
        for idx, url in enumerate(playlist.video_urls, 1):
            future = self.thread_pool.submit(self.process_episode, drama_name, idx, url)
            futures.append(future)
            
        # Wait for all futures to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                logger.error(f"Episode processing generated an exception: {exc}")
    
    def process_all_dramas(self):
        """Process all dramas"""
        logger.info("Starting video download process for all dramas")
        
        # Create futures for each drama
        futures = []
        for drama_name in dramas:
            future = self.thread_pool.submit(self.process_drama, drama_name)
            futures.append((drama_name, future))
            
        # Wait for all dramas to complete
        for drama_name, future in futures:
            try:
                future.result()
                logger.info(f"Completed processing drama: {drama_name}")
            except Exception as exc:
                logger.error(f"Drama {drama_name} processing generated an exception: {exc}")
        
        logger.info("Completed processing all dramas")


if __name__ == "__main__":
    # Create directories if they don't exist
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    downloader = VideoDownloader()
    downloader.process_all_dramas() 
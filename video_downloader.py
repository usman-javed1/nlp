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
S3_BUCKET = "nlpbucket21"  # S3 bucket for storing content
S3_COORD_BUCKET = "socsite"  # S3 bucket for coordination
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
        print("Initializing Video Downloader...")
        print(f"Instance ID: {INSTANCE_ID}")
        print(f"S3 Content Bucket: {S3_BUCKET}")
        print(f"S3 Coordination Bucket: {S3_COORD_BUCKET}")
        print(f"Using max {MAX_THREADS} threads for episode processing")
        
        self.s3_client = boto3.client('s3')
        self.lock = threading.Lock()
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS)
        print("S3 client and thread pool initialized")
        
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
        
        print(f"Attempting to acquire job: {drama_name} Episode {episode_idx}")
        
        try:
            # Try to check if someone else is processing this job
            try:
                response = self.s3_client.get_object(Bucket=S3_COORD_BUCKET, Key=job_key)
                existing_job = json.loads(response['Body'].read().decode('utf-8'))
                
                # If job is complete or less than 1 hour old, don't take it
                if existing_job["status"] == "complete":
                    print(f"Job already completed by {existing_job.get('instance', 'unknown')}")
                    return False
                
                # If job is processing but older than 1 hour, assume it failed
                if time.time() - existing_job["start_time"] < 3600:
                    print(f"Job in progress by {existing_job.get('instance', 'unknown')}, started {int((time.time() - existing_job['start_time'])/60)} minutes ago")
                    return False
                else:
                    print(f"Found stale job from {existing_job.get('instance', 'unknown')}, taking over")
                    
            except ClientError as e:
                if e.response['Error']['Code'] != 'NoSuchKey':
                    logger.error(f"S3 error: {str(e)}")
                    print(f"S3 error checking job status: {e.response['Error']['Code']}")
                    return False
                else:
                    print(f"No existing job found, claiming this job")
            
            # Claim the job
            print(f"Writing job claim to S3: {job_key}")
            self.s3_client.put_object(
                Bucket=S3_COORD_BUCKET, 
                Key=job_key,
                Body=json.dumps(job_data).encode('utf-8')
            )
            print(f"Successfully claimed job: {drama_name} Episode {episode_idx}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to acquire job: {str(e)}")
            print(f"Exception during job acquisition: {str(e)}")
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
        
        print(f"Marking job as complete: {drama_name} Episode {episode_idx}")
        try:
            self.s3_client.put_object(
                Bucket=S3_COORD_BUCKET, 
                Key=job_key,
                Body=json.dumps(job_data).encode('utf-8')
            )
            print(f"Successfully marked job complete in S3")
            return True
        except Exception as e:
            logger.error(f"Failed to mark job complete: {str(e)}")
            print(f"Failed to mark job complete: {str(e)}")
            return False
    
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
    
    def upload_to_s3(self, local_path, s3_key):
        """Upload file to S3 bucket"""
        print(f"Starting S3 upload: {local_path} → s3://{S3_BUCKET}/{s3_key}")
        try:
            file_size = os.path.getsize(local_path) / (1024 * 1024)  # Size in MB
            print(f"File size: {file_size:.2f} MB")
            
            print("Uploading to S3...")
            self.s3_client.upload_file(local_path, S3_BUCKET, s3_key)
            
            # Generate the S3 URL for the uploaded file
            s3_url = f"https://{S3_BUCKET}.s3.amazonaws.com/{s3_key}"
            
            logger.info(f"Uploaded {local_path} to s3://{S3_BUCKET}/{s3_key}")
            print(f"Upload successful!")
            print(f"File URL: {s3_url}")
            
            return True
        except Exception as e:
            logger.error(f"Failed to upload to S3: {str(e)}")
            print(f"S3 upload failed: {str(e)}")
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
        s3_video_key = f"dramas/{drama_name}/{episode_filename}"
        
        # Create local directory if it doesn't exist
        if not os.path.exists(os.path.dirname(local_path)):
            print(f"Creating directory: {os.path.dirname(local_path)}")
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        logger.info(f"Processing {drama_name} episode {idx}: {url}")
        print(f"\n--------- PROCESSING {drama_name} Episode {idx} ---------")
        print(f"YouTube URL: {url}")
        print(f"Local path: {local_path}")
        print(f"S3 destination: s3://{S3_BUCKET}/{s3_video_key}")
        
        # Download the video
        print("\n--- VIDEO DOWNLOAD PHASE ---")
        if self.download_video(url, local_path):
            logger.info(f"Successfully downloaded {episode_filename}")
            print(f"✓ Downloaded: {episode_filename}")
            
            # Upload to S3
            print("\n--- S3 UPLOAD PHASE ---")
            if self.upload_to_s3(local_path, s3_video_key):
                logger.info(f"Successfully uploaded {episode_filename} to S3")
                
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
                        s3_transcript_key = f"transcripts/{drama_name}/{os.path.basename(transcript_file)}"
                        print(f"Uploading to S3: s3://{S3_BUCKET}/{s3_transcript_key}")
                        if self.upload_to_s3(transcript_file, s3_transcript_key):
                            transcript_count += 1
                            logger.info(f"Uploaded transcript {transcript_file}")
                    else:
                        print(f"Transcript not found: {transcript_file}")
                
                if transcript_count == 0:
                    print("No transcript files found")
                else:
                    print(f"✓ Uploaded {transcript_count} transcript files")
                
                # Delete local file after successful upload
                print("\n--- CLEANUP PHASE ---")
                try:
                    print(f"Deleting local file: {local_path}")
                    os.remove(local_path)
                    logger.info(f"Deleted local file {local_path}")
                    print(f"✓ Cleaned up local file: {local_path}")
                except Exception as e:
                    logger.warning(f"Failed to delete local file: {str(e)}")
                    print(f"⚠ Failed to delete local file: {str(e)}")
                
                # Mark job as complete
                print("\n--- JOB COMPLETION PHASE ---")
                self.mark_job_complete(drama_name, idx)
                print(f"✓ Marked job as complete")
                print(f"--------- FINISHED {drama_name} Episode {idx} ---------\n")
                return True
            else:
                logger.error(f"Failed to upload {episode_filename} to S3")
                print(f"✗ Failed to upload {episode_filename} to S3")
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
    
    print("Testing AWS credentials and S3 access...")
    try:
        s3_client = boto3.client('s3')
        response = s3_client.list_buckets()
        print(f"AWS credentials valid. Found {len(response['Buckets'])} buckets.")
        
        # Check if our buckets exist
        bucket_names = [bucket['Name'] for bucket in response['Buckets']]
        if S3_BUCKET in bucket_names:
            print(f"✓ Content bucket exists: {S3_BUCKET}")
        else:
            print(f"⚠ Content bucket not found: {S3_BUCKET}")
            
        if S3_COORD_BUCKET in bucket_names:
            print(f"✓ Coordination bucket exists: {S3_COORD_BUCKET}")
        else:
            print(f"⚠ Coordination bucket not found: {S3_COORD_BUCKET}")
            
    except Exception as e:
        print(f"AWS credential test failed: {str(e)}")
    
    print("\nInitializing downloader...")
    downloader = VideoDownloader()
    
    print("\nStarting drama processing...")
    downloader.process_all_dramas()
    
    print(f"\nScript completed at: {time.strftime('%Y-%m-%d %H:%M:%S')}") 
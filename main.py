import boto3
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()

# Configuration
BUCKET_NAME = os.getenv("BUCKET_NAME", "koala-minting-nft-temp")
S3_KEY_PREFIX = os.getenv("S3_KEY_PREFIX", "")
LOCAL_DOWNLOAD_DIR = os.getenv("LOCAL_DOWNLOAD_DIR", "/tmp/koala-migrator/raw")
LOCAL_COMPRESS_DIR = os.getenv("LOCAL_COMPRESS_DIR", "/tmp/koala-migrator/compressed")
RCLONE_ARGS = os.getenv("RCLONE_ARGS", "")
GDRIVE_REMOTE = os.getenv("GDRIVE_REMOTE")  # rclone remote and folder path for Google Drive
MAX_WORKERS = os.cpu_count()

# Initialize S3 client
s3 = boto3.client('s3')


def download_from_gdrive(filename):
    rclone_command = [
        'rclone',
        'copy',
        os.path.join(GDRIVE_REMOTE, filename),
        LOCAL_DOWNLOAD_DIR
    ]
    if RCLONE_ARGS != '':
        rclone_command.append(RCLONE_ARGS)
    subprocess.run(rclone_command, check=True)


def compress_video(input_path, output_path):
    ffmpeg_command = [
        'ffmpeg',
        '-y',
        '-i', input_path,
        '-vcodec', 'libx265',  # Example codec for compression
        '-preset', 'faster',
        '-crf', '13',  # Constant Rate Factor, adjust for desired quality/size
        output_path
    ]
    subprocess.run(ffmpeg_command, check=True)


def upload_file(local_path, bucket_name, key):
    s3.upload_file(local_path, bucket_name, key)


def process_video(filename):
    try:
        local_download_path = os.path.join(LOCAL_DOWNLOAD_DIR, filename)
        local_compressed_path = os.path.join(LOCAL_COMPRESS_DIR, filename)

        # Download video from Google Drive
        print(f"Downloading {filename} from Google Drive...")
        download_from_gdrive(filename)

        # Compress video
        print(f"Compressing {local_download_path}...")
        compress_video(local_download_path, local_compressed_path)

        # Upload compressed video to S3
        print(f"Uploading {filename} to S3...")
        upload_file(local_compressed_path, BUCKET_NAME, os.path.join(S3_KEY_PREFIX, filename))

        # Clean up local files
        os.remove(local_download_path)
        os.remove(local_compressed_path)

        print(f"Processed {filename} successfully.")
    except Exception as e:
        print(f"Error processing {filename}: {e}")


def get_gdrive_files():
    rclone_command = [
        'rclone',
        'lsf',
        GDRIVE_REMOTE,
        '--include', '*.mp4'
    ]

    if RCLONE_ARGS != '':
        rclone_command.append(RCLONE_ARGS)

    result = subprocess.run(rclone_command, capture_output=True, text=True)
    return result.stdout.splitlines()


def process_videos():
    # Get list of video files in Google Drive
    print("Listing videos in Google Drive...")
    filenames = get_gdrive_files()

    # Process videos in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_video, filename) for filename in filenames]

        for future in as_completed(futures):
            future.result()


if __name__ == "__main__":
    if not os.path.exists(LOCAL_DOWNLOAD_DIR):
        os.makedirs(LOCAL_DOWNLOAD_DIR)
    if not os.path.exists(LOCAL_COMPRESS_DIR):
        os.makedirs(LOCAL_COMPRESS_DIR)

    process_videos()

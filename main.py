import os
import io
import sys
import concurrent.futures
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.service_account import Credentials
from tqdm import tqdm
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Replace 'YOUR_SERVICE_ACCOUNT_FILE.json' with the name of your service account JSON file
SERVICE_ACCOUNT_FILE = "BlissfulBonsai.json"
SCOPES = ["https://www.googleapis.com/auth/drive"]

def authenticate_service_account():
    """Authenticate using a service account file."""
    try:
        credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        return build("drive", "v3", credentials=credentials)
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        sys.exit(1)

def download_file(service, file_id, file_name, destination, progress_bar=None):
    """Download a single file from Google Drive."""
    try:
        file_path = os.path.join(destination, file_name)
        # Skip if file already exists to avoid redundant downloads
        if os.path.exists(file_path):
            logger.info(f"Skipping existing file: {file_path}")
            return

        request = service.files().get_media(fileId=file_id)
        with io.FileIO(file_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
                if progress_bar:
                    progress_bar.update(1)  # Update progress bar
            logger.info(f"Downloaded: {file_path}")
    except Exception as e:
        logger.error(f"Failed to download {file_name}: {e}")

def download_folder(service, folder_id, destination, max_workers=4):
    """Recursively download a folder from Google Drive using multiple threads."""
    try:
        query = f"'{folder_id}' in parents and trashed = false"
        results = service.files().list(q=query, fields="files(id, name, mimeType)").execute()
        items = results.get("files", [])

        if not items:
            logger.info("No files found in the folder.")
            return

        # Separate files and folders
        files = [(item["id"], item["name"]) for item in items if item["mimeType"] != "application/vnd.google-apps.folder"]
        folders = [(item["id"], item["name"]) for item in items if item["mimeType"] == "application/vnd.google-apps.folder"]

        # Create progress bar for files
        with tqdm(total=len(files), desc="Downloading files", unit="file") as pbar:
            # Download files concurrently using ThreadPoolExecutor
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(download_file, service, file_id, file_name, destination, pbar)
                    for file_id, file_name in files
                ]
                # Wait for all downloads to complete
                concurrent.futures.wait(futures)

        # Recursively process subfolders
        for folder_id, folder_name in folders:
            folder_path = os.path.join(destination, folder_name)
            os.makedirs(folder_path, exist_ok=True)
            logger.info(f"Processing folder: {folder_name}")
            download_folder(service, folder_id, folder_path, max_workers)

    except Exception as e:
        logger.error(f"Error processing folder: {e}")

def main():
    if len(sys.argv) < 3:
        print("Usage: python main.py <FOLDER_LINK> <DESTINATION_PATH> [MAX_WORKERS]")
        sys.exit(1)

    folder_link = sys.argv[1]
    destination = sys.argv[2]
    max_workers = int(sys.argv[3]) if len(sys.argv) > 3 else 4

    # Extract folder ID from the link
    try:
        if "drive.google.com" in folder_link:
            folder_id = folder_link.split("/")[5].split("?")[0]
        else:
            logger.error("Invalid Google Drive folder link.")
            sys.exit(1)
    except IndexError:
        logger.error("Invalid Google Drive folder link format.")
        sys.exit(1)

    # Authenticate and initialize the Drive service
    service = authenticate_service_account()

    # Create the destination directory if it doesn't exist
    os.makedirs(destination, exist_ok=True)

    # Download the folder
    logger.info(f"Starting download to {destination} with {max_workers} workers...")
    download_folder(service, folder_id, destination, max_workers)
    logger.info("Download completed.")

if __name__ == "__main__":
    main()

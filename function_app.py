import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient

STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=logsstorage0;AccountKey=5UK6AEEBlxTUvOJqHRmT4VEY3rYkK6ZJDrkZ6+K+Xm3RYCGhE0ihqXNzIP+7dM5qhwPe+B1el67E+ASt2SHr/w==;EndpointSuffix=core.windows.net"
TARGET_CONTAINER = "$logs"

def main(event: func.EventGridEvent):
    try:
        logging.info(f"Processing event: {event.id}")

        # Extract Storage Account URL
        blob_url = event.get_json().get("data", {}).get("url", "")
        if not blob_url:
            logging.error("No blob URL found in event")
            return

        blob_name = blob_url.split("/")[-1]
        container_name = blob_url.split("/")[-2]  # Extract container name

        # Initialize Blob Service Client
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        source_container_client = blob_service_client.get_container_client(container_name)
        target_container_client = blob_service_client.get_container_client(TARGET_CONTAINER)

        # Download Append Blob Content
        source_blob_client = source_container_client.get_blob_client(blob_name)
        blob_data = source_blob_client.download_blob().readall()

        # Upload as Block Blob
        target_blob_client = target_container_client.get_blob_client(blob_name)
        target_blob_client.upload_blob(blob_data, blob_type="BlockBlob", overwrite=True)

        logging.info(f"Successfully converted {blob_name} to Block Blob")

        # Delete original Append Blob
        source_blob_client.delete_blob()
        logging.info(f"Deleted original Append Blob: {blob_name}")

    except Exception as e:
        logging.error(f"Error processing event: {str(e)}")

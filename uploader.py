import os
import tempfile
import time
import boto3
from botocore.exceptions import BotoCoreError, ClientError
import httpx
import json
import requests
from urllib.parse import urljoin, quote
from datetime import datetime
import xml.etree.ElementTree as ET
import base64
import hashlib
from smart_open import open as smart_open
import httpx
from urllib.parse import urlparse
import threading

from botocore.client import Config
from boto3.s3.transfer import TransferConfig
from io import BytesIO
import logging
import io

# Set up logging
logging.basicConfig(level=None)
logger = logging.getLogger(__name__)

def upload_graph_file_to_s3(graph_file_url: str, curate_path: str, curate_details: dict, graph_access_token: str, file_size: str, multipart_threshold: int = 100 * 1024 * 1024) -> dict:
    """
    Transfer a file from the graph api to Curate. Uses streaming for files under multipart threshold,
    for files over multipart threshold it uses standard download/upload pattern

    Args:
        graph_file_url (str): The URL of the file in Graph API.
        s3_presigned_data (dict): Contains 'url', 'headers', and 'path' for S3 upload.
        graph_access_token (str): Access token for Graph API.
        curate_access_token (str): API key for the Curate instance
        file_size (str): Size of the file to transfer in bytes. 
        multipart_threshold (int): File size threshold for multipart upload (default is 100MB).
        

    Returns:
        dict: A dictionary containing:
            - 'success' (bool): Whether the operation was successful.
            - 'status' (int): HTTP status code of the S3 upload (if successful).
            - 'item' (str): The path of the uploaded item.
            - 'error' (str): Error message (if unsuccessful).
    """
    graph_headers = {
        'Authorization': f'Bearer {graph_access_token}',
        'Accept': 'application/octet-stream'
    }
    
    if int(file_size) >= multipart_threshold:
        return upload_large_graph_file_to_s3(graph_file_url,  graph_access_token, curate_path, curate_details)
    else:
        s3_presigned_data = build_presigned_put_url(curate_path, curate_details)
        try:
            
            with httpx.stream('GET', graph_file_url, headers=graph_headers, follow_redirects=True) as graph_response:
                return stream_graph_file_to_s3(graph_response, s3_presigned_data)

        except httpx.HTTPStatusError as e:
            print(f"HTTP error in stream_graph_file_to_s3: {e}")
            return {'success': False, 'error': str(e), 'item': s3_presigned_data['path']}


def upload_large_graph_file_to_s3(graph_file_url: str, graph_access_token: str, curate_path: str, curate_details: dict) -> dict:
    """
    Uploads a large graph file to S3 using multipart uploads 
    Downloads the file first and then uploads (no streaming)

    Args:
        graph_file_url (str): The URL of the file in Graph API.
        graph_access_token (str): Access token for the Graph API.
        curate_path (str): The path for the file in S3.
        curate_details (dict): Contains 'siteUrl' and 'apiKey' for S3 upload.
    """
    try:
        print("multi upload details: ", graph_file_url, curate_path, curate_details)

        bucket = "io"
        key = f"quarantine/SharePoint Uploads/{curate_path}"
        endpoint_url = f"https://{curate_details.siteUrl}"

        headers = {'Authorization': f'Bearer {graph_access_token}'}
        graph_response = requests.get(graph_file_url, headers=headers)
        graph_response.raise_for_status()

        total_file_size = len(graph_response.content)
        logger.info(f"File size from Graph API: {total_file_size}")

        if total_file_size == 0:
            raise ValueError("File size is 0")

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(graph_response.content)
            temp_file_path = temp_file.name
            temp_file.flush()
            os.fsync(temp_file.fileno())

        if os.path.getsize(temp_file_path) != total_file_size:
            raise ValueError(f"File size mismatch: expected {total_file_size}, got {os.path.getsize(temp_file_path)}")

        config = Config(
            signature_version='s3v4',
            s3={'addressing_style': 'path'}
        )
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=curate_details.apiKey,
            aws_secret_access_key='gatewaysecret',
            config=config
        )

        with open(temp_file_path, 'rb') as file_data:
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=file_data,
                ContentType='application/octet-stream'
            )

        os.unlink(temp_file_path)

        logger.info(f"Upload completed successfully for {key}")
        return {'success': True, 'status': 200, 'item': curate_path}

    except Exception as e:
        logger.error(f"Error in upload_large_graph_file_to_s3: {str(e)}", exc_info=True)
        return {'success': False, 'status': 500, 'error': str(e), 'item': curate_path}



def stream_graph_file_to_s3(graph_response: httpx.Response, s3_presigned_data: dict) -> dict:
    """
    Stream a file from Graph API to S3 using single-part upload.
    """
    try:
        s3_url = s3_presigned_data['url']
        s3_headers = s3_presigned_data['headers']
        s3_headers['Content-Length'] = graph_response.headers.get('Content-Length', '0')

        with httpx.stream('PUT', s3_url, data=graph_response.iter_bytes(), headers=s3_headers) as s3_response:
            s3_response.raise_for_status()
            return {'success': True, 'status': s3_response.status_code, 'item': s3_presigned_data['path']}
    except httpx.HTTPStatusError as e:
        print(f"HTTP error in stream_graph_file_to_s3: {e}")
        return {'success': False, 'error': str(e), 'item': s3_presigned_data['path']}
    except httpx.RequestError as e:
        print(f"Request error in stream_graph_file_to_s3: {e}")
        return {'success': False, 'error': str(e), 'item': s3_presigned_data['path']}
    
    
def build_presigned_put_url(path: str, curate_details: dict, user_meta: dict = {}) -> dict:
    """
    Generate a pre-signed URL for S3 file upload.

    Args:
        path (str): The path where the file will be stored in S3.
        curate_details (dict): Contains 'siteUrl' and 'apiKey' for the Curate site.
        user_meta (dict, optional): User metadata to be attached to the S3 object.

    Returns:
        dict: A dictionary containing:
            - 'path' (str): The original path.
            - 'url' (str): The pre-signed URL for S3 upload.
            - 'headers' (dict): Headers to be used with the pre-signed URL.

    Raises:
        BotoCoreError: If there's an error in the AWS SDK.
        ClientError: If there's an error in the S3 client operations.
    """
    target_path = f"quarantine/SharePoint Uploads/{path}"
    params = {
        'Bucket': "io",
        'Key': target_path,
        'ContentType': "application/octet-stream",
        'Metadata': user_meta
    }

    s3 = boto3.client('s3', 
                      aws_access_key_id='gateway', 
                      aws_secret_access_key='gatewaysecret', 
                      region_name='eu-west-1', 
                      endpoint_url=f"https://{curate_details.siteUrl}")
    try:
        signed_url = s3.generate_presigned_url('put_object', Params=params, ExpiresIn=3600)
        return {
            'path': path,
            'url': signed_url,
            'headers': {
                'X-Pydio-Bearer': curate_details.apiKey,
                'Content-Type': 'application/octet-stream'
            }
        }
    except (BotoCoreError, ClientError) as e:
        print(f"Error generating presigned URL: {e}")
        raise

def create_empty_folder(folder_name: str, curate_details: dict) -> dict:
    """
    Create an empty folder in the specified path.

    Args:
        folder_name (str): Name of the folder to be created.
        curate_details (dict): Contains 'siteUrl' and 'apiKey' for the Curate site.

    Returns:
        dict: A dictionary containing:
            - 'success' (bool): Whether the operation was successful.
            - 'data' (dict): Response data from the API (if successful).
            - 'error' (str): Error message (if unsuccessful).
    """
    url = f"https://{curate_details.siteUrl}/a/tree/create" 
    headers = {
        'Authorization': f'Bearer {curate_details.apiKey}',
        'Content-Type': 'application/json'
    }
    
    body = {
        "Nodes": [
            {
                "Path": f"quarantine/SharePoint Uploads/{folder_name}"
            }
        ],
        "Recursive": True
    }
    
    try:
        response = requests.post(url, headers=headers, json=body)
        response.raise_for_status()
        return {'success': True, 'data': response.json()}
    except requests.RequestException as e:
        print(f"Error in create_empty_folder: {str(e)}")
        return {'success': False, 'error': str(e)}

def update_user_meta(node_uuid: str, curate_details: dict, namespace_value_pairs: dict) -> dict:
    """
    Update user metadata for a specific node.

    Args:
        node_uuid (str): UUID of the node to update.
        curate_details (dict): Contains 'siteUrl' and 'apiKey' for the Curate site.
        namespace_value_pairs (dict): Key-value pairs of namespaces and their values to update.

    Returns:
        dict: A dictionary containing:
            - 'success' (bool): Whether the operation was successful.
            - 'data' (dict): Response data from the API (if successful).
            - 'error' (str): Error message (if unsuccessful).
    """
    url = f"https://{curate_details.siteUrl}/a/user-meta/update"

    metadatas = [
        {
            "NodeUuid": node_uuid,
            "Namespace": namespace,
            "JsonValue": f"\"{value}\"",
            "Policies": [
                {"Action": "READ", "Effect": "allow", "Subject": "*"},
                {"Action": "WRITE", "Effect": "allow", "Subject": "*"}
            ]
        }
        for namespace, value in namespace_value_pairs.items()
    ]

    body = {
        "MetaDatas": metadatas,
        "Operation": "PUT"
    }

    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {curate_details.apiKey}"}
    
    try:
        response = requests.put(url, json=body, headers=headers)
        response.raise_for_status()
        return {'success': True, 'data': response.json()}
    except requests.RequestException as e:
        print(f"Error in update_user_meta: {str(e)}")
        return {'success': False, 'error': str(e)}
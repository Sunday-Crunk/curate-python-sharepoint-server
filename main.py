import datetime

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi import BackgroundTasks, HTTPException

from graph_tools import AzureAuth, update_drive_item_metadata, list_files_in_folder
from uploader import build_presigned_put_url, upload_graph_file_to_s3, create_empty_folder, update_user_meta
from pydantic import BaseModel
from typing import List

app = FastAPI()

# CORS settings (if needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://penwernlimited.sharepoint.com"],  # Lets have an env variable for the allowed SharePoint site
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PORT = 3030

# Hardcoded Azure credentials (for development use only)
CLIENT_ID = ""
CLIENT_SECRET = ""
TENANT_ID = ""

# Environment variable versions 
# CLIENT_ID = os.environ.get('AZURE_CLIENT_ID')
# CLIENT_SECRET = os.environ.get('AZURE_CLIENT_SECRET')
# TENANT_ID = os.environ.get('AZURE_TENANT_ID')


# Define the models

class CurateDetails(BaseModel):
    apiKey: str
    siteUrl: str

class SharepointDetails(BaseModel):
    drivePath: str
    siteId: str

class UploadItem(BaseModel):
    id: str
    spId: str
    driveId: str
    name: str
    fileSize: str
    type: str  # You can use an enum if the types are predefined, e.g., Enum("File", "Folder")

class UserInfo(BaseModel):
    name: str
    email: str

class SharePointPackage(BaseModel):
    curateDetails: CurateDetails
    sharepointDetails: SharepointDetails
    uploadItems: List[UploadItem]
    userInfo: UserInfo


upload_container_name_format = lambda: f"SharePointUpload_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"

@app.post("/uploadSharePointPackage")
async def upload_sharepoint_package(data: SharePointPackage, background_tasks: BackgroundTasks) -> dict:
    """
    Upload a SharePoint package to the upload container.
    """
    try:
        background_tasks.add_task(upload_task, data)
        return {"success": True, "message": "Upload task initiated successfully."}
    except Exception as e:
        print(f"Error in upload_sharepoint_package: {str(e)}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")


async def upload_task(data: SharePointPackage) -> None:
    """
    Upload a SharePoint package to the upload container.

    Args:
        data (SharePointPackage): The validated SharePoint package data.
    """
    try:
        curate_details = data.curateDetails
        sharepoint_details = data.sharepointDetails
        upload_items = data.uploadItems
        user_details = data.userInfo

        # Assuming you have these constants or you can inject them
        auth_client = AzureAuth(CLIENT_ID, TENANT_ID, CLIENT_SECRET)
        access_token = auth_client.get_access_token()

        if not upload_items:
            print("No items to upload.")
            return

        container_folder_name = upload_container_name_format()

        for item in upload_items:
            try:
                # Update metadata and process items
                update_drive_item_metadata(sharepoint_details.siteId, item.driveId, item.spId, item.name, {"PreservationStatus": "Initiating"}, access_token)

                if item.type == 'Folder':
                    process_folder(sharepoint_details, curate_details, user_details, item, container_folder_name, access_token)
                    status = "Success"
                else:
                    result = process_item(sharepoint_details, curate_details, user_details, item, None, access_token, container_folder_name)
                    status = "Success" if result.get('success') else f"Failed: {result.get("error")}"
                    if not result.get('success'):
                        print(f"Error processing item {item.name}: {result.get('message')}")
                
                update_drive_item_metadata(sharepoint_details.siteId, item.driveId, item.spId, item.name, {"PreservationStatus": status}, access_token)
            except Exception as e:
                print(f"Error processing item {item.name}: {str(e)}")
                update_drive_item_metadata(sharepoint_details.siteId, item.driveId, item.spId, item.name, {"PreservationStatus": f"Failed: {str(e)}"}, access_token)

        print("All items processed.")
    except Exception as e:
        print(f"Error in upload_task: {str(e)}")
        raise


def process_item(sharepoint_details: dict, curate_details: dict, user_details: dict, item: dict, folder: str, access_token: str, container_folder_name: str) -> dict:
    """
    Process a single item in the SharePoint package.

    Args:
        sharepoint_details (dict): Contains 'siteId' and 'siteUrl' for the SharePoint site.
        curate_details (dict): Contains 'siteUrl' and 'apiKey' for the Curate site.
        user_details (dict): Contains 'name' and 'email' for the user.
        item (dict): Details of the item to process.
        folder (str): Name of the folder where the item is located.
        access_token (str): Access token for the SharePoint site.
        container_folder_name (str): Name of the container folder where the item will be uploaded.

    Returns:
        dict: A dictionary containing:
            - 'success' (bool): Whether the operation was successful.
            - 'item' (dict): Details of the uploaded item.
            - 'message' (str): Error message (if unsuccessful).
    """
    try:
        # if file is larger than 10gb, reject it with message explaining you should use the Soteria+ command line client or sftp for uploads over 10gb.
        if int(item.fileSize) > 1 * 1024 * 1024 * 1024:
            raise Exception("File size is too large. Please use the Soteria+ command line client or sftp for uploads over 10gb.")
        stream_url = f"https://graph.microsoft.com/v1.0/sites/{sharepoint_details.siteId}/drives/{item.driveId}/items/{item.id}/content"
        r = create_empty_folder(container_folder_name, curate_details)
        if not r['success']:
            raise Exception(f"Error creating scoped upload folder: {r.get('message')}")

        folder_uuid = r['data']['Children'][0]['Uuid']
        if not folder_uuid:
            raise Exception("Error creating scoped upload folder: No UUID returned")

        r = update_user_meta(folder_uuid, curate_details, {"usermeta-contributor": f"{user_details.name}:{user_details.email}"})
        if not r['success']:
            raise Exception(f"Error updating user meta: {r.get('message')}")

        path = f"{container_folder_name}/{folder}/{item.name}" if folder else f"{container_folder_name}/{item.name}"
        

        upload_result = upload_graph_file_to_s3(stream_url, path, curate_details, access_token, item.fileSize)

        if not upload_result['success']:
            raise Exception(f"Upload failed: {upload_result.get('error')}")

        return {'success': True, 'item': upload_result['item']}
    except Exception as e:
        print(f"Error in process_item: {str(e)}")
        return {'success': False, 'message': str(e)}

def process_folder(sharepoint_details: dict, curate_details: dict, user_details: dict, folder_item: dict, container_folder_name: str, access_token: str) -> None:
    """
    Process a folder in the SharePoint package. Microsoft Graph doesn't support downloading
    whole folders, so we need to recursively process each item in the folder.

    Args:
        sharepoint_details (dict): Contains 'siteId' and 'siteUrl' for the SharePoint site.
        curate_details (dict): Contains 'siteUrl' and 'apiKey' for the Curate site.
        user_details (dict): Contains 'name' and 'email' for the user.
        folder_item (dict): Details of the folder to process.
        container_folder_name (str): Name of the container folder where the folder will be uploaded.
        access_token (str): Access token for the SharePoint site.
    """
    try:
        r = create_empty_folder(f"{container_folder_name}", curate_details)
        if not r['success']:
            raise Exception(f"Error creating container folder: {r.get('message')}")

        folder_uuid = r['data']['Children'][0]['Uuid']
        if not folder_uuid:
            raise Exception("Error creating container folder: No UUID returned")

        r = update_user_meta(folder_uuid, curate_details, {"usermeta-contributor": f"{user_details.name}:{user_details.email}"})
        if not r['success']:
            raise Exception(f"Error updating user meta for container folder: {r.get('message')}")

        r = create_empty_folder(f"{container_folder_name}/{folder_item.name}", curate_details)
        if not r['success']:
            raise Exception(f"Error creating subfolder: {r.get('message')}")

        folder_uuid = r['data']['Children'][0]['Uuid']
        if not folder_uuid:
            raise Exception("Error creating subfolder: No UUID returned")
        print("ballsack: ", folder_item)
        files = list_files_in_folder(sharepoint_details.siteId, folder_item.driveId, folder_item.id, access_token)
        for file in files['value']:
            if 'folder' in file and file['folder']['childCount'] > 0:
                process_folder(sharepoint_details, curate_details, user_details, file, container_folder_name, access_token)
            elif 'folder' not in file:
                item = UploadItem(id=file['id'], spId=file['id'], driveId=file['parentReference']['driveId'], name=file['name'], type='File')
                 
                process_item(sharepoint_details, curate_details, user_details, item, folder_item.name, access_token, container_folder_name)
    except Exception as e:
        print(f"Error in process_folder: {str(e)}")
        raise
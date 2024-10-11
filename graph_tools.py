import requests

class AzureAuth:
    """
    Authenticate with Azure Active Directory using OAuth2 and obtain an access token.
        
    Attributes:
    token_url (str): The URL for the OAuth2 token endpoint.
    client_id (str): The client ID for the application.
    client_secret (str): The client secret for the application.
    resource (str): The resource for the application.
    """
    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        """
        Initialize the AzureAuth class.

        Args:
            tenant_id (str): The tenant ID for the Azure AD application.
            client_id (str): The client ID for the Azure AD application.
            client_secret (str): The client secret for the Azure AD application.
        """
        self.token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        self.client_id = client_id
        self.client_secret = client_secret
        self.resource = 'https://graph.microsoft.com/.default'

    def get_access_token(self) -> str:
        """
        Get an access token for the Azure AD application.

        Returns:
            str: The access token.
        """
        payload = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'scope': self.resource
        }
        response = requests.post(self.token_url, data=payload, headers={'Content-Type': 'application/x-www-form-urlencoded'})
        if response.status_code == 200:
            data = response.json()
            with open('access_token.txt', 'w') as f:
                f.write(data['access_token'])
            return data['access_token']
        else:
            response.raise_for_status()

def get_drive_id_by_library_name(site_id: str, library_name: str, access_token: str) -> str:
    """
    Get the drive ID for a given library name in a SharePoint site using Microsoft Graph API.

    Args:
    site_id (str): The ID of the SharePoint site.
    library_name (str): The name of the library to get the drive ID for.
    access_token (str): The OAuth2 access token for Microsoft Graph API.

    Returns:
    str: The drive ID for the library.
    """
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    print("data: ", data)
    for drive in data['value']:
        if drive['name'] == library_name:
            print("drive: ", drive)
            return drive['id']
    raise ValueError("No drive found with the name:", library_name)

def list_files_in_folder(site_id: str, drive_id: str, folder_id: str, access_token: str) -> dict:
    """
    Lists files in a folder in a SharePoint document library using Microsoft Graph API.

    Args:
    site_id (str): The ID of the SharePoint site.
    drive_id (str): The ID of the document library (considered as a drive).
    folder_id (str): The ID of the folder to list files in.
    access_token (str): The OAuth2 access token for Microsoft Graph API.

    Returns:
    dict: A dictionary containing the list of files or an error message.
    """
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{folder_id}/children"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises an HTTPError for bad requests (4XX or 5XX)
        return response.json()  # Returns the JSON response containing the list of files
    except requests.exceptions.HTTPError as err:
        return {'error': str(err), 'details': response.json()}

def list_files_in_library(site_id: str, drive_id: str, access_token: str) -> dict:
    """
    Lists files in a SharePoint document library using Microsoft Graph API.

    Args:
    site_id (str): The ID of the SharePoint site.
    drive_id (str): The ID of the document library (considered as a drive).
    access_token (str): The OAuth2 access token for Microsoft Graph API.

    Returns:
    dict: A dictionary containing the list of files or an error message.
    """
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root/children"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises an HTTPError for bad requests (4XX or 5XX)
        return response.json()  # Returns the JSON response containing the list of files
    except requests.exceptions.HTTPError as err:
        return {'error': str(err), 'details': response.json()}
    
def search_files_by_filename(site_id: str, drive_id: str, access_token: str, filename: str) -> dict:
    """
    Search for files in a SharePoint document library by filename using Microsoft Graph API's search capability.

    Args:
    site_id (str): The ID of the SharePoint site.
    drive_id (str): The ID of the document library (considered as a drive).
    access_token (str): The OAuth2 access token for Microsoft Graph API.
    filename (str): Name of the file to search for.

    Returns:
    dict: A dictionary containing the search results or an error message.
    """
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root/search(q='{filename}')"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises an HTTPError for bad requests (4XX or 5XX)
        return response.json()  # Returns the JSON response containing the search results
    except requests.exceptions.HTTPError as err:
        return {'error': str(err), 'details': response.json() if response.content else "No additional details available."}


def update_drive_item_metadata(site_id: str, drive_id: str, item_id: str, item_name: str, metadata: dict, access_token: str) -> dict:
    """
    Update metadata for an item in a Microsoft OneDrive or SharePoint drive using the Microsoft Graph API.

    Args:
    drive_id (str): The unique identifier for the drive.
    item_id (str): The unique identifier for the drive item to update.
    metadata (dict): A dictionary containing the metadata fields and values to update.
    access_token (str): The OAuth access token with appropriate permissions.

    Returns:
    dict: The response from the Microsoft Graph API.
    """
    # Check if search_files_by_filename function works correctly
    print("Searching for files by filename:", search_files_by_filename(site_id, drive_id, access_token, item_name))
    print("Item ID:", item_id)
    
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{item_id}/listItem/fields"
    
    print("URL:", url)
    print("Metadata to update:", metadata)
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }
    
    # Perform the PATCH request to update metadata
    response = requests.patch(url, headers=headers, json=metadata)
    
    if response.status_code == 200:
        print("Metadata update successful")
        return {'success': True, 'data': response.json()}
    else:
        print("Metadata update failed")
        print("Status code:", response.status_code)
        try:
            error_response = response.json()
            print("Error response:", error_response)
        except ValueError:
            error_response = response.text
            print("Error response (non-JSON):", error_response)
        
        return {'success': False, 'error': error_response}
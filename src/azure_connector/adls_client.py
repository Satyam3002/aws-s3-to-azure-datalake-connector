"""
Azure Data Lake Storage Gen2 Connector Module
Handles connection to Azure ADLS Gen2 and file upload operations.
"""

from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import AzureError, ResourceExistsError
from typing import Tuple
import os
import hashlib


class ADLSConnector:
    """Simple class to handle Azure ADLS Gen2 operations."""
    
    def __init__(self, account_name: str, account_key: str):
        """
        Initialize ADLS client with credentials.
        
        Args:
            account_name: Azure Storage Account name
            account_key: Azure Storage Account Key
        """
        self.account_name = account_name
        self.account_key = account_key
        self.service_client = None
    
    def connect(self) -> Tuple[bool, str]:
        """
        Create ADLS service client connection.
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            account_url = f"https://{self.account_name}.dfs.core.windows.net"
            
            self.service_client = DataLakeServiceClient(
                account_url=account_url,
                credential=self.account_key
            )
            return True, "Connected successfully"
        except Exception as e:
            return False, f"Connection error: {str(e)}"
    
    def test_connection(self, container_name: str) -> Tuple[bool, str]:
        """
        Test if we can access the container (simple health check).
        
        Args:
            container_name: Name of the container/filesystem
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        if self.service_client is None:
            return False, "Not connected. Call connect() first."
        
        try:
            file_system_client = self.service_client.get_file_system_client(file_system=container_name)
            # Try to get properties (will raise error if container doesn't exist or no access)
            properties = file_system_client.get_file_system_properties()
            return True, f"Successfully connected to container '{container_name}'"
        except AzureError as e:
            if "ContainerNotFound" in str(e) or "does not exist" in str(e):
                return False, f"Container '{container_name}' not found. Please create it first in Azure Portal."
            elif "Authorization" in str(e) or "permission" in str(e).lower():
                return False, "Access denied. Check your Azure credentials."
            else:
                return False, f"Error: {str(e)}"
        except Exception as e:
            return False, f"Connection test failed: {str(e)}"
    
    def upload_file(self, container_name: str, local_file_path: str, remote_path: str) -> Tuple[bool, str]:
        """
        Upload a file to ADLS Gen2.
        
        Args:
            container_name: Name of the container/filesystem
            local_file_path: Path to local file to upload
            remote_path: Path in ADLS (e.g., '/raw_data/filename.parquet')
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        if self.service_client is None:
            return False, "Not connected. Call connect() first."
        
        # Check if local file exists
        if not os.path.exists(local_file_path):
            return False, f"Local file not found: {local_file_path}"
        
        try:
            # Get file system (container) client
            file_system_client = self.service_client.get_file_system_client(file_system=container_name)
            
            # Ensure the directory path exists (create if needed)
            # Remove filename from path to get directory
            directory_path = os.path.dirname(remote_path).lstrip('/')
            if directory_path:
                directory_client = file_system_client.get_directory_client(directory_path)
                try:
                    directory_client.create_directory()
                except ResourceExistsError:
                    # Directory already exists, that's fine
                    pass
            
            # Get file client for the target file
            file_client = file_system_client.get_file_client(remote_path)
            
            # Upload file
            file_size = os.path.getsize(local_file_path)
            with open(local_file_path, 'rb') as local_file:
                file_client.upload_data(
                    data=local_file.read(),
                    overwrite=True
                )
            
            return True, f"Successfully uploaded to {remote_path} ({file_size / (1024*1024):.2f} MB)"
            
        except AzureError as e:
            if "ContainerNotFound" in str(e):
                return False, f"Container '{container_name}' not found."
            elif "Authorization" in str(e):
                return False, "Access denied. Check your Azure credentials."
            else:
                return False, f"Azure error: {str(e)}"
        except Exception as e:
            return False, f"Error uploading file: {str(e)}"

    def compute_remote_md5(self, container_name: str, remote_path: str, chunk_size: int = 1024 * 1024) -> Tuple[bool, str]:
        """
        Compute MD5 for a remote ADLS file by streaming its contents.
        
        Args:
            container_name: Name of the container/filesystem
            remote_path: Path to file in ADLS (e.g., '/raw_data/filename.parquet')
            chunk_size: Size of chunks to read (default 1MB)
        
        Returns:
            Tuple of (success: bool, md5_hex or error message: str)
        """
        if self.service_client is None:
            return False, "Not connected. Call connect() first."
        
        try:
            file_system_client = self.service_client.get_file_system_client(file_system=container_name)
            file_client = file_system_client.get_file_client(remote_path)
            
            # Download file in chunks to compute MD5
            md5 = hashlib.md5()
            downloader = file_client.download_file()
            
            # Read file in chunks to avoid memory issues with large files
            while True:
                chunk = downloader.read(chunk_size)
                if not chunk:
                    break
                md5.update(chunk)
            
            return True, md5.hexdigest()
            
        except AzureError as e:
            if "NotFound" in str(e) or "does not exist" in str(e):
                return False, f"File not found: {remote_path}"
            elif "Authorization" in str(e):
                return False, "Access denied. Check your Azure credentials."
            else:
                return False, f"Azure error while reading remote file: {str(e)}"
        except Exception as e:
            return False, f"Error computing remote MD5: {str(e)}"
    
    def create_directory_if_not_exists(self, container_name: str, directory_path: str) -> Tuple[bool, str]:
        """
        Create a directory in ADLS if it doesn't exist.
        
        Args:
            container_name: Name of the container/filesystem
            directory_path: Directory path (e.g., 'raw_data')
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        if self.service_client is None:
            return False, "Not connected. Call connect() first."
        
        try:
            file_system_client = self.service_client.get_file_system_client(file_system=container_name)
            directory_client = file_system_client.get_directory_client(directory_path)
            
            try:
                directory_client.create_directory()
                return True, f"Directory '{directory_path}' created"
            except ResourceExistsError:
                return True, f"Directory '{directory_path}' already exists"
        except Exception as e:
            return False, f"Error creating directory: {str(e)}"


"""
AWS S3 Connector Module
Handles connection to S3 and file listing operations.
"""

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from typing import List, Dict, Optional, Tuple


class S3Connector:
    """Simple class to handle AWS S3 operations."""
    
    def __init__(self, access_key_id: str, secret_access_key: str, region: str):
        """
        Initialize S3 client with credentials.
        
        Args:
            access_key_id: AWS Access Key ID
            secret_access_key: AWS Secret Access Key
            region: AWS region (e.g., 'us-east-1')
        """
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.region = region
        self.s3_client = None
    
    def connect(self) -> Tuple[bool, str]:
        """
        Create S3 client connection.
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                region_name=self.region
            )
            return True, "Connected successfully"
        except Exception as e:
            return False, f"Connection error: {str(e)}"
    
    def list_files(self, bucket_name: str, file_extensions: Optional[List[str]] = None) -> Tuple[bool, List[Dict], str]:
        """
        List files in S3 bucket.
        
        Args:
            bucket_name: Name of S3 bucket
            file_extensions: Optional list of file extensions to filter (e.g., ['csv', 'json', 'parquet'])
                           If None, returns all files
        
        Returns:
            Tuple of (success: bool, files: List[Dict], message: str)
            Each file dict contains: {'name': str, 'size': int, 'size_mb': float}
        """
        if self.s3_client is None:
            return False, [], "Not connected. Call connect() first."
        
        if file_extensions is None:
            # Default: CSV, JSON, Parquet
            file_extensions = ['csv', 'json', 'parquet']
        
        try:
            # List objects in bucket
            response = self.s3_client.list_objects_v2(Bucket=bucket_name)
            
            if 'Contents' not in response:
                return True, [], "Bucket is empty or no files found."
            
            files = []
            for obj in response['Contents']:
                file_key = obj['Key']  # File path/name in bucket
                file_size = obj['Size']  # Size in bytes
                
                # Filter by extension (case-insensitive)
                file_ext = file_key.split('.')[-1].lower() if '.' in file_key else ''
                if file_ext in [ext.lower() for ext in file_extensions]:
                    files.append({
                        'name': file_key,
                        'size': file_size,
                        'size_mb': round(file_size / (1024 * 1024), 2)  # Convert to MB
                    })
            
            # Sort by filename
            files.sort(key=lambda x: x['name'])
            
            message = f"Found {len(files)} file(s) matching extensions: {', '.join(file_extensions)}"
            return True, files, message
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                return False, [], f"Bucket '{bucket_name}' does not exist."
            elif error_code == 'AccessDenied':
                return False, [], "Access denied. Check your AWS credentials and permissions."
            else:
                return False, [], f"AWS error: {error_code} - {str(e)}"
        except NoCredentialsError:
            return False, [], "AWS credentials not found or invalid."
        except Exception as e:
            return False, [], f"Error listing files: {str(e)}"
    
    def test_connection(self, bucket_name: str) -> Tuple[bool, str]:
        """
        Test if we can access the bucket (simple health check).
        
        Args:
            bucket_name: Name of S3 bucket
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        if self.s3_client is None:
            return False, "Not connected. Call connect() first."
        
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            return True, f"Successfully connected to bucket '{bucket_name}'"
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                return False, f"Bucket '{bucket_name}' not found."
            elif error_code == '403':
                return False, "Access denied. Check your credentials."
            else:
                return False, f"Error: {error_code}"
        except Exception as e:
            return False, f"Connection test failed: {str(e)}"
    
    def download_file(self, bucket_name: str, file_key: str, local_path: str) -> Tuple[bool, str]:
        """
        Download a file from S3 to local path.
        
        Args:
            bucket_name: Name of S3 bucket
            file_key: Key (path) of file in S3 bucket
            local_path: Local file path where file will be saved
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        if self.s3_client is None:
            return False, "Not connected. Call connect() first."
        
        try:
            # Download file from S3
            self.s3_client.download_file(bucket_name, file_key, local_path)
            return True, f"Successfully downloaded {file_key}"
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                return False, f"File '{file_key}' not found in bucket."
            elif error_code == 'AccessDenied':
                return False, "Access denied. Check your AWS credentials and permissions."
            else:
                return False, f"AWS error: {error_code} - {str(e)}"
        except Exception as e:
            return False, f"Error downloading file: {str(e)}"


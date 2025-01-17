import boto3
import logging
from typing import Optional, Dict, Any
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class S3Client:
    """AWS S3 client wrapper."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize S3 client.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.client = boto3.client(
            's3',
            region_name=config['storage'].get('aws_region'),
            aws_access_key_id=config['storage'].get('aws_access_key'),
            aws_secret_access_key=config['storage'].get('aws_secret_key')
        )

    def upload_file(
        self,
        local_path: str,
        bucket: str,
        s3_path: str,
        extra_args: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Upload file to S3.
        
        Args:
            local_path: Local file path
            bucket: S3 bucket name
            s3_path: S3 object key
            extra_args: Additional S3 upload arguments
        
        Returns:
            bool: True if upload successful
        """
        try:
            self.client.upload_file(
                local_path,
                bucket,
                s3_path,
                ExtraArgs=extra_args or {}
            )
            return True
        except ClientError as e:
            logger.error(f"Error uploading to S3: {str(e)}")
            return False

    def download_file(
        self,
        bucket: str,
        s3_path: str,
        local_path: str
    ) -> bool:
        """Download file from S3.
        
        Args:
            bucket: S3 bucket name
            s3_path: S3 object key
            local_path: Local file path
        
        Returns:
            bool: True if download successful
        """
        try:
            self.client.download_file(bucket, s3_path, local_path)
            return True
        except ClientError as e:
            logger.error(f"Error downloading from S3: {str(e)}")
            return False

    def list_objects(
        self,
        bucket: str,
        prefix: Optional[str] = None
    ) -> List[str]:
        """List objects in S3 bucket.
        
        Args:
            bucket: S3 bucket name
            prefix: Object prefix filter
        
        Returns:
            List[str]: List of object keys
        """
        try:
            paginator = self.client.get_paginator('list_objects_v2')
            
            objects = []
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix or ''):
                if 'Contents' in page:
                    objects.extend([obj['Key'] for obj in page['Contents']])
                    
            return objects
        except ClientError as e:
            logger.error(f"Error listing S3 objects: {str(e)}")
            return []

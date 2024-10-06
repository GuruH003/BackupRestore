from .views import *
import re
import os
from minio import Minio

def InitializeClient(minioEndPoint, minioAccessKey, minioSecretKey, minioSecure):
    try:
        client = Minio(
            endpoint=minioEndPoint,
            access_key=minioAccessKey,
            secret_key=minioSecretKey,
            secure=minioSecure,
        )
        return client
    except Exception as e:
        print(f"Error initializing MinIO client: {e}")
        return None

def human_readable_size(size):
    if size < 1024:
        return f"{size} B"
    elif size < 1024 ** 2:
        return f"{size / 1024:.2f} KB"
    elif size < 1024 ** 3:
        return f"{size / (1024 ** 2):.2f} MB"
    elif size < 1024 ** 4:
        return f"{size / (1024 ** 3):.2f} GB"
    else:
        return f"{size / (1024 ** 4):.2f} TB"


def ListBuckets(client):
    try:
        buckets = client.list_buckets()
        resp=[]
        total_storage_size = 0
        if buckets:
            for bucket in buckets:
                total_size = 0
                for obj in client.list_objects(bucket.name, recursive=True):
                    total_size += obj.size
                
                total_storage_size += total_size
                    
                resp.append({"name" : bucket.name, 
                             "estimated_size": human_readable_size(total_size)})
            
            total_storage_size_human = human_readable_size(total_storage_size)
            return {
            "buckets": resp,
            "total_storage_size": total_storage_size_human
            }
            # return resp
    except Exception as e:
        print(f"Error checking connection: {e}")
        return False

def ValidateBucketName(name):
    pattern = r'^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$'
    return bool(re.match(pattern, name))


def EnsureBucketExists(client, name):
    if not ValidateBucketName(name):
        print(f"Bucket name '{name}' is invalid.")
        return False
    try:
        if client.bucket_exists(name):
            print(f"Bucket '{name}' exists.")
            return True
    except Exception as e:
        print(f"Error ensuring bucket '{name}' exists: {e}")
        return False


def DownloadFilesFromBucket(bucketName, downloadDir, client):
    bucketName = bucketName if bucketName else bucketName
    if not downloadDir or not bucketName:
        print("MINIO_DOWNLOAD_DIR or MINIO_DOWNLOAD_BUCKET_NAME is not set.")
        return False
    try:
        if not EnsureBucketExists(client, bucketName):
            print(f"Bucket '{bucketName}' does not exist.")
            return False
        objects = client.list_objects(bucketName, recursive=True)
        
        for obj in objects:
            localFilePath = os.path.join(downloadDir, obj.object_name)
            localDir = os.path.dirname(localFilePath)
            
            if not os.path.exists(localDir):
                os.makedirs(localDir)
            
            client.fget_object(bucketName, obj.object_name, localFilePath)
            print(f"Downloaded '{obj.object_name}' from bucket '{bucketName}' to '{localFilePath}'.")
        return True
    except Exception as e:
        print(f"Error downloading files from bucket '{bucketName}': {str(e)}")
        return False

def UploadFiles(client, bucketName, filePath):
    try:
        if EnsureBucketExists(client, bucketName):
            for root, dirs, files in os.walk(filePath):
                for file in files:
                    file_path = os.path.join(root, file)
                    minio_path = os.path.relpath(file_path, filePath)

                    print(f"Uploading file '{file_path}' to bucket '{bucketName}' as '{minio_path}'")
                    with open(file_path, 'rb') as data:
                        client.put_object(bucketName, minio_path, data, os.path.getsize(file_path))
            return True
        else:
            print(f"Failed to upload file '{filePath}' to bucket '{bucketName}'.")
            return False
    except Exception as e:
        print(f"Error uploading file '{filePath}' to bucket '{bucketName}': {str(e)}")
        return False


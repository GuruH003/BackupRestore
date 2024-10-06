from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import os
from dotenv import load_dotenv
from .utils import *


load_dotenv()

# MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")
# MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
# MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
# MINIO_SECURE = os.environ.get("MINIO_SECURE")

# client = InitializeClient(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE)

class BucketList(APIView):
    def get(self, request):
        
        minioEndpoint = request.query_params.get('minio_endpoint',None)
        minioAccessKey = request.query_params.get('minio_access_key',None)
        minioSecretKey = request.query_params.get('minio_secret_key',None)
        minioSecure = False
        client = InitializeClient(minioEndpoint, minioAccessKey, minioSecretKey, minioSecure)
        
        if client:
            bucketList = ListBuckets(client)
            if bucketList:
                payload = {
                    "status":True,
                    "message":"List of buckets in object store",
                    "data":bucketList['buckets'],
                    "total_storage_size": bucketList['total_storage_size'],
                    "error":None
                }
                return Response(payload, status=status.HTTP_200_OK)
            else:
                payload = {
                    "status":False,
                    "message":"Error in listing of buckets in object store",
                    "data":None,
                    "error":"Empty bucket cant be listed."
                }
                return Response(payload, status=status.HTTP_400_BAD_REQUEST)
        else:
            payload = {
                    "status":False,
                    "message":"Cant able to connect Minio.",
                    "data":None,
                    "error":"Connection Failed."
                }
            return Response(payload, status=status.HTTP_400_BAD_REQUEST)
    
class MinioBackup(APIView):
    def post(self, request):
        minioEndpoint = request.data.get('minio_endpoint',None)
        minioAccessKey = request.data.get('minio_access_key',None)
        minioSecretKey = request.data.get('minio_secret_key',None)
        minioSecure = False
        client = InitializeClient(minioEndpoint, minioAccessKey, minioSecretKey, minioSecure)
        
        bucketName = request.data.get("bucket_name",None)
        backupPath = request.data.get("backup_path",None)
        
        if bucketName and backupPath:
            if DownloadFilesFromBucket(bucketName,backupPath,client):
                payload = {
                    "status":True,
                    "message":"Files from the object store are downloaded succesfully.",
                    "data":backupPath,
                    "error":None
                }
                return Response(payload, status=status.HTTP_200_OK)
            else:
                payload = {
                    "status":False,
                    "message":"Error in downloading files from bucket.",
                    "data":None,
                    "error":"Either path or bucekt name not provided."
                }
                return Response(payload, status=status.HTTP_400_BAD_REQUEST)

class MinioRestore(APIView):
    def post(self, request):
        minioEndpoint = request.data.get('minio_endpoint',None)
        minioAccessKey = request.data.get('minio_access_key',None)
        minioSecretKey = request.data.get('minio_secret_key',None)
        minioSecure = False
        client = InitializeClient(minioEndpoint, minioAccessKey, minioSecretKey, minioSecure)
        
        backupPath = request.data.get("file_path",None)
        bucketName = request.data.get("bucket_name",None)
        if backupPath:
            if UploadFiles(client, bucketName,backupPath):
                payload = {
                    "status":True,
                    "message":"Files restored to object store succesfully from path.",
                    "data":backupPath,
                    "error":None
                }
                return Response(payload, status=status.HTTP_200_OK)
        else:
            payload = {
                    "status":False,
                    "message":"Provide valid backup path.",
                    "data":None,
                    "error":"Backup path not provided by user."
                }
            return Response(payload, status=status.HTTP_400_BAD_REQUEST)
    
        
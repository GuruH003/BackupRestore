from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from cassandra.cluster import Cluster
import os
from dotenv import load_dotenv
from .utils import *


load_dotenv()

# END_POINTS = os.environ.get("SCYLLA_ENDPOINTS").split(',')
# HOST = os.environ.get("SCYLLA_HOST")
# USERNAME = os.environ.get("SCYLLA_USER")
# PASSWORD = os.environ.get("SCYLLA_PASSWORD")

class ScyllaBackup(APIView):
    def get(self, request):
        endPoints = request.query_params.get('end_points').split(',')
        scyllaHost = request.query_params.get('scylla_host',None)
        scyllaPort = request.query_params.get('scylla_port',None)
        scyllaPassword = request.query_params.get('scylla_password',None)
        scyllaUser = request.query_params.get('scylla_user',None)
        
        cluster = Cluster(endPoints, port=9042)
        session = cluster.connect()
        keySpaces = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        
        keySpaceNames = []
        totalSize = 0
        for row in keySpaces:
            keySpaceName=row.keyspace_name
            try:
                # Get estimated backup size for each keyspace
                estimatedSizeDict, _ = GetEstimatedBackupSize(scyllaHost, scyllaUser, scyllaPassword, [keySpaceName])
                estimatedSize = estimatedSizeDict.get(keySpaceName, "0 B")
                sizeInBytes = ConvertToBytes(estimatedSize)  # You will need a helper function for this
                totalSize += sizeInBytes 
            except Exception as e:
                estimatedSize = f"Error estimating size: {str(e)}"

            # Add the keyspace and its size to the list
            keySpaceNames.append({
                'keyspace_name': keySpaceName,
                'estimated_size': estimatedSize
            })
        
        formattedTotalSize = FormatSize(totalSize)
        
        session.shutdown()
        cluster.shutdown()
        
        payload = {
                "status": True,
                "message": "List of available keyspaces in the cluster",
                "data": keySpaceNames,
                "total_size": formattedTotalSize,
                "error": None
            }
        return Response(payload, status=status.HTTP_200_OK)
    
    def post(self,request):
        data = request.data
        
        scyllaHost = data.get('scylla_host',None)
        scyllaPort = data.get('scylla_port',None)
        scyllaPassword = data.get('scylla_password',None)
        scyllaUser = data.get('scylla_user',None)
        
        keySpaceName = data.get("keyspace_name", None)
        tableName = data.get("table_name", None)
        backupPath = data.get("backup_path",None)
        
        if backupPath:
            if keySpaceName is not None and tableName is not None:
                try:
                    snapShotPaths = CaptureDataForSingleTable(scyllaHost, scyllaUser, scyllaPassword, keySpaceName, tableName, backupPath)
                    payload = {
                        "status": True,
                        "message": "Backup done successfully",
                        "data": snapShotPaths,
                        "error": None
                    }
                    return Response(payload, status=status.HTTP_200_OK)
                except Exception as e:
                    payload = {
                        "status": False,
                        "message": "Backup failed due to an error.",
                        "data": None,
                        "error": str(e)
                    }
                    return Response(payload, status=status.HTTP_400_BAD_REQUEST)
            else:
                payload = {
                    "status": False,
                    "message": "Backup not initiated.",
                    "data": None,
                    "error": "Either keyspace or table not provided"
                }
                return Response(payload, status=status.HTTP_400_BAD_REQUEST)
        else:
            payload = {
                "status": False,
                "message": "Backup path not provided.",
                "data": None,
                "error": "Backup wont proceed."
            }
            return Response(payload, status=status.HTTP_406_NOT_ACCEPTABLE)

class ScyllaKeyspaceAndTable(APIView):
    def get(self, request):
        scyllaHost = request.query_params.get('scylla_host',None)
        scyllaPassword = request.query_params.get('scylla_password',None)
        scyllaUser = request.query_params.get('scylla_username',None)
        keySpaceName = request.query_params.get("keyspace_name", None)
        tableName = request.query_params.get("table_name", None)
        
        if keySpaceName and tableName:
            if KeyspaceExists(scyllaHost, scyllaUser, scyllaPassword, keySpaceName):
                if CheckTablesExist(scyllaHost, scyllaUser, scyllaPassword, keySpaceName, tableName):
                    payload = {
                        "status": True,
                        "message": "Table exists.",
                        "data": tableName,
                        "error": None
                    }
                    return Response(payload, status=status.HTTP_200_OK)
                else:
                    payload = {
                        "status": True,
                        "message": "Table does not exists.",
                        "data": tableName,
                        "error": None
                    }
                    return Response(payload, status=status.HTTP_200_OK)
            else:
                payload = {
                    "status": False,
                    "message": "Keyspace does not exists.",
                    "data": keySpaceName,
                    "error": None
                }
                return Response(payload, status=status.HTTP_400_BAD_REQUEST)
        else:
            payload = {
                "status": False,
                "message": "Keyspace name and table name are required.",
                "data": None,
                "error": None
            }
            return Response(payload, status=status.HTTP_400_BAD_REQUEST)

class ScyllaRestoreForSingleTable(APIView):
    def post(self, request):
        data = request.data

        scyllaHost = data.get('scylla_host',None)
        scyllaPassword = data.get('scylla_password',None)
        scyllaUser = data.get('scylla_username',None)
        
        backupFile = data.get("backup_file", None)
        keyspace = data.get("keyspace",None)
        tableName = data.get("tablename",None)
        
        try:
            if RestoreDataForSingleTable(scyllaHost, scyllaUser, scyllaPassword, keyspace, tableName, backupFile):
                payload = {
                    "status": True,
                    "message": f"Restoration of table {tableName} completed successfully. Please restart ScyllaDB to reflect the newly backed-up data.",
                    "data": None,
                    "error": None
                }
                return Response(payload, status=status.HTTP_200_OK)
            else:
                payload = {
                   "status": False,
                    "message": "Restoration failed",
                    "data":None,
                    "error": "Check if the keyspace and table name are correct."
                }
                return Response(payload, status=status.HTTP_200_OK)
        except Exception as e:
                payload = {
                    "status": False,
                    "message": "Restoration failed due to an error.",
                    "data": None,
                    "error": str(e),
                }
                return Response(status=status.HTTP_400_BAD_REQUEST)
    
        
class ScyllaBackupKeyspace(APIView):
    def post(self, request):
        scyllaHost = request.data.get('scylla_host',None)
        scyllaPassword = request.data.get('scylla_password',None)
        scyllaUser = request.data.get('scylla_username',None)
        
        keyspaceName = request.data.get("keyspace_name",None)
        backupPath = request.data.get("backup_path",None)
        if backupPath:
            if keyspaceName:
                path = CaptureKeySpaceSnapshot(scyllaHost, scyllaUser, scyllaPassword, keyspaceName, backupPath)
                payload = {
                    "status": True,
                    "message": "Backup done",
                    "data": path,
                    "error": None
                }
                return Response(payload, status=status.HTTP_200_OK)
            else:
                payload = {
                    "status": False,
                    "message": "Backup cannot proceed.",
                    "data": None,
                    "error": "keyspace not provided."
                }
                return Response(payload, status=status.HTTP_400_BAD_REQUEST)
        else:
            payload = {
                "status": False,
                "message": "Backup path not provided.",
                "data": None,
                "error": "Backup wont proceed."
            }
            return Response(payload, status=status.HTTP_406_NOT_ACCEPTABLE)

class ScyllaRestoreKeyspace(APIView):
    def post(self, request):
        scyllaHost = request.data.get('scylla_host',None)
        scyllaPassword = request.data.get('scylla_password',None)
        scyllaUser = request.data.get('scylla_username',None)
        
        keyspaceName = request.data.get("keyspace_name",None)
        backupFile = request.data.get("backup_file",None)
        
        if backupFile:
            RestoreKeySpaceFromLocal(scyllaHost, scyllaUser, scyllaPassword, keyspaceName, backupFile)
            payload = {
                    "status": True,
                    "message": f"Restore done for keyspaces {keyspaceName}. Please restart ScyllaDB to reflect the newly backed-up data.",
                    "data": "path",
                    "error": None
                }
            return Response(payload, status=status.HTTP_200_OK)
        else:
            payload = {
                "status": False,
                "message": "Backup path not provided.",
                "data": None,
                "error": "Backup wont proceed."
            }
            return Response(payload, status=status.HTTP_406_NOT_ACCEPTABLE)
    
    def put(self, request):
        scyllaHost = request.data.get('scylla_host',None)
        scyllaPassword = request.data.get('scylla_password',None)
        scyllaUser = request.data.get('scylla_username',None)
        
        restart = request.data.get("restart",None)
        if restart:
            StartScylla(scyllaHost,scyllaUser,scyllaPassword)
            payload = {
                    "status": True,
                    "message": "Scylladb has been restarted.",
                    "data": None,
                    "error": None,
                }
            return Response(payload, status=status.HTTP_200_OK)
        
    
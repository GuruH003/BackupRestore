from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import os
from dotenv import load_dotenv
from .utils import *
from elasticsearch import Elasticsearch
import json
import re

load_dotenv()

class ViewIndexes(APIView):
    def get(self, request):
        elasticUrl = request.query_params.get('elastic_url',None)
        es = Elasticsearch([elasticUrl])
        
        try:
            IndexList = IndexListAndSize(es)
            payload = {
                "status":True,
                "message":"List of indexes in cluster",
                "data":IndexList,
                "error":None
            }
            return Response(payload, status=status.HTTP_200_OK)
        except Exception as e:
            payload = {
                "status":False,
                "message":"Error in listing indexes.",
                "data":str(e),
                "error":"Failed to fetch indexes."
            }
            return Response(payload, status=status.HTTP_400_BAD_REQUEST)

class BackupIndexes(APIView):
    def post(self, request):
        elasticUrl = request.data.get('elastic_url',None)
        es = Elasticsearch([elasticUrl])
        
        indexName = request.data.get("index_name",None)
        backupPath = request.data.get("backup_path",None)
        
        if indexName:
            response = es.search(index=indexName, body={"query": {"match_all": {}}}, size=10000)
            documents = response['hits']['hits']
            
            if backupPath:
                backupPath = os.path.join(backupPath, f'backup_{indexName}.json')
            else:
                payload = {
                    "status":False,
                    "message":"Backup path not provided.",
                    "data":None,
                    "error":"Backup path not provided."
                }
                return Response(payload, status=status.HTTP_404_NOT_FOUND)
            
            WriteToJsonFile(documents, backupPath)

            payload = {
                "status": True,
                "message": f'Backup of index {indexName} done.',
                "path": backupPath,
                "error": None
            }
            return Response(payload, status=status.HTTP_200_OK)
        
        else:
            allIndexes = es.indices.get_alias(index='*')
            indexList = list(allIndexes.keys())
            allDocuments = []

            for index in indexList:
                response = es.search(index=index, body={"query": {"match_all": {}}}, size=10000)
                allDocuments.extend(response['hits']['hits'])

            if backupPath:
                backupPath = os.path.join(backupPath, 'backup_all_indexes.json')
            else:
                payload = {
                    "status":False,
                    "message":"Backup path not provided.",
                    "data":None,
                    "error":"Backup path not provided."
                }
                return Response(payload, status=status.HTTP_404_NOT_FOUND)

            WriteToJsonFile(allDocuments, backupPath)

            payload = {
                "status": True,
                "message": 'Backup of all indexes done.',
                "path": backupPath,
                "error": None
            }
            return Response(payload, status=status.HTTP_200_OK)
    
    def get(self, request):
        elasticUrl = request.query_params.get('elastic_url',None)
        es = Elasticsearch([elasticUrl])
        indexName = request.query_params.get("index_name",None)
        if indexName:
            indexSize=GetSizeOfIndex(es, indexName)
            payload = {
                "status": True,
                "message": 'Estimated size.',
                "size": indexSize,
                "error": None
            }
            return Response(payload, status=status.HTTP_200_OK)
        else:
            indexSize=GetSizeOfIndex(es)
            payload = {
                "status": True,
                "message": 'Estimated size.',
                "size": indexSize,
                "error": None
            }
            return Response(payload, status=status.HTTP_200_OK)

class RestoreIndexes(APIView):
    def post(self, request):
        elasticUrl = request.data.get('elastic_url',None)
        es = Elasticsearch([elasticUrl])
        backupPath = request.data.get("backup_path",None)
        indexName = request.data.get("index_name",None)
        
        if backupPath:
            if indexName:
                if not os.path.exists(backupPath):
                    payload = {
                        "status": False,
                        "message": "Backup file not found.",
                        "data": None,
                        "error": f"No backup found for index {indexName}."
                    }
                    return Response(payload, status=status.HTTP_404_NOT_FOUND)

                with open(backupPath, 'r') as f:
                    documents = json.load(f)
                    for doc in documents:
                        doc_id = doc.get('_id')
                        doc_body = doc.get('_source')
                        
                        es.index(index=indexName, id=doc_id, body=doc_body)

                payload = {
                    "status": True,
                    "message": f'Index {indexName} restored successfully.',
                    "data": None,
                    "error": None
                }
                return Response(payload, status=status.HTTP_200_OK)
            else:
                allIndexes = []
                with open(backupPath, 'r') as f:
                    documents = json.load(f)
                    for doc in documents:
                        indexName = doc.get('_index')
                        if not es.indices.exists(index=indexName):
                            payload = {
                                "status": False,
                                "message": "Index does not exist.",
                                "data": None,
                                "error": f"The index {indexName} does not exist."
                            }
                            return Response(payload, status=status.HTTP_404_NOT_FOUND)

                        doc_id = doc.get('_id')
                        doc_body = doc.get('_source')
                        es.index(index=indexName, id=doc_id, body=doc_body)
                        if indexName not in allIndexes:
                            allIndexes.append(indexName)

                payload = {
                    "status": True,
                    "message": 'All indexes restored successfully.',
                    "data": allIndexes,
                    "error": None
                }
                return Response(payload, status=status.HTTP_200_OK)
        else:
            payload = {
                "status": False,
                "message": "Invalid backup file.",
                "data":None,
                "error": "Backup path not found"
            }
            return Response(payload, status=status.HTTP_404_NOT_FOUND)
        
    def put(self, request):
        elasticUrl = request.data.get('elastic_url',None)
        es = Elasticsearch([elasticUrl])
        indexes = request.data.get("index_name", [])
        responses = []
        valid_index_name_pattern = re.compile(r'^[a-zA-Z0-9_]+$') 
        for indexeName in indexes:
            if not valid_index_name_pattern.match(indexeName):
                responses.append({
                    "status": False,
                    "message": "Invalid index name. Must only contain letters, numbers, and underscores.",
                    "index": indexeName,
                    "error": None
                })
                continue 
            
            try:
                if es.indices.exists(index=indexeName):
                    responses.append({
                        "status": False,
                        "message": "Index already exists.",
                        "index": indexeName,
                        "error": None
                    })
                    continue
                
                es.indices.create(index=indexeName)
                payload={
                    "status": True,
                    "message": "Index created successfully.",
                    "index": indexeName,
                    "error": None
                }
                return Response(payload, status=status.HTTP_201_CREATED)

            except Exception as e:
                responses.append({
                    "status": False,
                    "message": "Index creation failed.",
                    "index": indexeName,
                    "error": str(e)
                })

        # Return the collected responses
        return Response(responses, status=status.HTTP_200_OK)


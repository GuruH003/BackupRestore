from .views import *
from math import log
from elasticsearch.helpers import bulk
import json

def human_readable_size(sizeInBytes):
    if sizeInBytes == 0:
        return "0 Bytes"
    sizeNames = ["Bytes", "KB", "MB", "GB", "TB"]
    index = min(int(log(sizeInBytes, 1024)), len(sizeNames) - 1)
    size = sizeInBytes / (1024 ** index)
    return f"{size:.2f} {sizeNames[index]}"

def IndexListAndSize(es):
    indexes = es.indices.get_alias(index='*')
    indexList = list(indexes.keys())
    
    indexStats = es.indices.stats(index=indexList)
    resp=[]
    for index in indexList:
        indexSizeBytes = indexStats['indices'][index]['total']['store']['size_in_bytes']
        resp.append({
            "index": index,
            "estimated_size": human_readable_size(indexSizeBytes),
        })
    return resp

def GetSizeOfIndex(es, indexName=None):
    if indexName:
        indexStats = es.indices.stats(index=indexName)
    else:
        indexStats = es.indices.stats()
        
    totalSize = sum(
        stat['total']['store']['size_in_bytes'] 
        for stat in indexStats['indices'].values()
    )
    Size = human_readable_size(totalSize)
    return Size

def WriteToJsonFile(documents, backupPath):
    with open(backupPath, 'w') as f:
        json.dump(documents, f)
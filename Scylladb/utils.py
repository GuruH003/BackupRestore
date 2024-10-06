from .views import *
import paramiko
from scp import SCPClient
import re
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from cassandra.cluster import Cluster
import os
import time

def CreateSshClient(server, port, user, password):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server, port, username=user, password=password)
    return client

def CheckDirExists(ssh, path):
    # Check if the directory exists on the remote server
    command = f'if [ -d "{path}" ]; then echo "exists"; fi'
    stdin, stdout, stderr = ssh.exec_command(command)
    return stdout.read().decode().strip() == "exists"

def CheckForErrors(stdout, stderr):
    stdoutOutput = stdout.read().decode().strip()
    stderrOutput = stderr.read().decode().strip()
    if stderrOutput:
        print(f"Error: {stderrOutput}")
        return False
    else:
        if stdoutOutput:
            print(stdoutOutput)
        return True

def FormatSize(sizeInBytes):
    if sizeInBytes < 1024:
        return f"{sizeInBytes} B"
    elif sizeInBytes < 1024**2:
        return f"{sizeInBytes / 1024:.2f} KB"
    elif sizeInBytes < 1024**3:
        return f"{sizeInBytes / 1024**2:.2f} MB"
    elif sizeInBytes < 1024**4:
        return f"{sizeInBytes / 1024**3:.2f} GB"
    else:
        return f"{sizeInBytes / 1024**4:.2f} TB"

def ConvertToBytes(sizeStr):
    # Convert human-readable sizes (like "500 MB") to bytes
    sizeStr = sizeStr.strip()
    size, unit = float(sizeStr[:-2]), sizeStr[-2:].upper()
    
    if unit == 'KB':
        return size * 1024
    elif unit == 'MB':
        return size * 1024 ** 2
    elif unit == 'GB':
        return size * 1024 ** 3
    elif unit == 'TB':
        return size * 1024 ** 4
    else:
        return size  # Assuming it's already in bytes if no unit

def GetEstimatedBackupSize(hostIP, username, password, keySpaces):
    if isinstance(keySpaces, str):
        keySpaces = [keySpaces]
        
    backupSizeEstimates = {}
    totalBackupSize = 0

    try:
        sshClient = CreateSshClient(hostIP, 22, username, password)
        for keySpace in keySpaces:
            print(keySpace)
            command = f'nodetool cfstats {keySpace}'
            stdin, stdout, stderr = sshClient.exec_command(command)

            stdoutOutput = stdout.read().decode()
            errorOutput = stderr.read().decode()
            # print(stdoutOutput)

            if errorOutput:
                # print(f"Error getting stats for keyspace '{keySpace}': {errorOutput.strip()}")
                backupSizeEstimates[keySpace] = "0 B"
                continue

            # Extract total size from the output
            totalSizeMatch = re.search(r'Space used \(total\):\s+(\d+)', stdoutOutput)
            # print(totalSizeMatch)
            
            if totalSizeMatch:
                totalSize = int(totalSizeMatch.group(1))
                formattedSize = FormatSize(totalSize) 
                backupSizeEstimates[keySpace] = formattedSize
                totalBackupSize += totalSize
                # print(f"Estimated backup size for keyspace '{keySpace}': {totalSize}")
            else:
                print(f"Could not find size information for keyspace '{keySpace}'.")
                backupSizeEstimates[keySpace] = "0 B"

    except Exception as e:
        print(f"Error estimating backup sizes: {e}")
        return {}

    finally:
        sshClient.close()
        formattedTotalSize = FormatSize(totalBackupSize)

    return backupSizeEstimates, formattedTotalSize

def KeyspaceExists(host, username, password, keyspace):
    authProvider = PlainTextAuthProvider(username, password)
    cluster = Cluster([host], auth_provider=authProvider)
    
    try:
        session = cluster.connect()
        query = f"SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = '{keyspace}'"
        result = session.execute(query)
        return len(result.current_rows) > 0
    except Exception as e:
        print(f"Error checking keyspace: {str(e)}")
        return False
    finally:
        cluster.shutdown()

def CheckTablesExist(host, username, password, keyspace, tableName):
    authProvider = PlainTextAuthProvider(username, password)
    cluster = Cluster([host], auth_provider=authProvider)
    
    try:
        session = cluster.connect(keyspace)  # Connect to the specified keyspace
        query = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{keyspace}' AND table_name = '{tableName}'"
        result = session.execute(query)
        return len(result.current_rows) > 0
    except Exception as e:
        print(f"Error checking table: {str(e)}")
        return False
    finally:
        cluster.shutdown()

def GetTableUuid(host, keyspace, tablename):
    # Connect to the ScyllaDB cluster
    cluster = Cluster([host])
    session = cluster.connect()

    # Switch to the desired keyspace
    session.set_keyspace(keyspace)

    # Query to get the UUID of the table
    query = "SELECT id FROM system_schema.tables WHERE keyspace_name = %s AND table_name = %s"
    statement = SimpleStatement(query)
    result = session.execute(statement, (keyspace, tablename))

    # Close the connection
    cluster.shutdown()

    # Check if we got a result
    if result and len(result.current_rows) > 0:
        return result[0].id  # Assuming `table_id` returns the UUID
    else:
        return None
    
def StartScylla(host, username, password):
    try:
        sshclient = CreateSshClient(host, 22, username, password)
        command = f'echo {password} | sudo -S systemctl restart scylla-server'
        print("Restarting Scylla service...")
        stdin, stdout, stderr = sshclient.exec_command(command)
        CheckForErrors(stdout, stderr)  
        return True
    except Exception as e:
        print(e)

def CopyFilesToDestination(host, username, password, sourcePath):
    temp_path = "/tmp/scylla_tmp"
    try:
        sshClient = CreateSshClient(host, 22, username, password)
        stdin, stdout, stderr = sshClient.exec_command(f"mkdir -p {temp_path}")
        CheckForErrors(stdout, stderr)
        
        with SCPClient(sshClient.get_transport()) as scp:
            # List files in the local source directory
            local_files = os.listdir(sourcePath)
            # print("Files to copy:", local_files)

            for file in local_files:
                local_file_path = os.path.join(sourcePath, file)
                remote_file_path = os.path.join(temp_path, file)
                print(f"Copying {file} to {remote_file_path}...")
                # Copy file to the remote destination
                try:
                    scp.put(local_file_path, remote_file_path)
                    print(f"Successfully copied {file} to {remote_file_path}")
                except Exception as e:
                    print(f"Error copying {file}: {e}")
    except Exception as e:
        print(f"SSH connection failed: {e}")
        return False
    finally:
        sshClient.close()

def ChangeOwnership(host, username, password):
    try:
        sshClient = CreateSshClient(host, 22, username, password)
        tempPath = "/tmp/scylla_tmp"
        command = f'echo {password} | sudo -S chown scylla:scylla {tempPath}/*'
        stdin, stdout, stderr = sshClient.exec_command(command)
        CheckForErrors(stdout, stderr)
            
    except Exception as e:
        print(f"SSH connection failed: {e}")
        return False
    finally:
        if sshClient:
            sshClient.close()

def MoveFiles(host, username, password, keyspace, tablename):
    try:
        with CreateSshClient(host, 22, username, password) as sshClient:
            tempPath = "/tmp/scylla_tmp"
            uuid= GetTableUuid(host, keyspace, tablename)
            tableid = str(uuid).replace("-", "")
            destinationPath = f"/var/lib/scylla/data/{keyspace}/{tablename}-{tableid}"

            command = f'echo {password} | sudo -S mv "{tempPath}"/* "{destinationPath}"'
            stdin, stdout, stderr = sshClient.exec_command(command)
            CheckForErrors(stdout, stderr)
            command = f'echo {password} | sudo -S rm -rf "{tempPath}"'
            stdin, stdout, stderr = sshClient.exec_command(command)
            CheckForErrors(stdout, stderr)
            
    except Exception as e:
        print(f"SSH connection failed: {e}")
        return False

def CaptureDataForSingleTable(host, username, password, keyspace, tablename, backupPath):
    sshClient = CreateSshClient(host, 22, username, password)
    
    snapshot_tag = f"{tablename}_snapshot"
    command = f"nodetool snapshot --tag {snapshot_tag} --table {tablename} {keyspace}"
    print("command",command)
    stdin, stdout, stderr = sshClient.exec_command(command)
    
    stdoutOutput = stdout.read().decode()
    errorOutput = stderr.read().decode()
    
    if errorOutput:
        print(f"Error during snapshot creation: {errorOutput}")
        return

    print(f"Snapshot created successfully: {stdoutOutput}")
    
    find_snapshot_command = f"find /var/lib/scylla/data/{keyspace}/{tablename}-*/snapshots/{snapshot_tag} -type d"
    
    stdin, stdout, stderr = sshClient.exec_command(find_snapshot_command)
    snapshot_dir = stdout.read().decode().strip()
    errorOutput = stderr.read().decode()
    
    if errorOutput or not snapshot_dir:
        print(f"Error finding snapshot directory: {errorOutput}")
        raise Exception(f"Snapshot directory not found: {errorOutput}")

    print(f"Snapshot directory found: {snapshot_dir}")
    
    scpClient = paramiko.SFTPClient.from_transport(sshClient.get_transport())
    
    if backupPath:
        if not os.path.exists(backupPath):
            os.makedirs(backupPath)
        
        for file in scpClient.listdir(snapshot_dir):
            remote_file_path = f"{snapshot_dir}/{file}"
            local_file_path = os.path.join(backupPath, file)
            
            scpClient.get(remote_file_path, local_file_path)
            print(f"Copied {file} to {backupPath}")
    
    scpClient.close()
    sshClient.close()
    print(f"Backup of table {tablename} completed successfully.")
    
    return backupPath if backupPath else None

def RestoreDataForSingleTable(host, username, password, keyspace, tablename, backupPath):
    try:
        sshClient= CreateSshClient(host, 22, username, password)
        
        if KeyspaceExists(host, username, password, keyspace):
            if CheckTablesExist(host, username, password, keyspace):
        
                CopyFilesToDestination(host, username, password, backupPath)
                time.sleep(2)
                ChangeOwnership(host, username, password)
                time.sleep(2)
                MoveFiles(host, username, password, keyspace, tablename)
            
                print("Data restoration completed successfully.")
                return True
    
    except Exception as e:
        print(f"An error occurred during restoration: {e}")
        return False
    finally:
        if sshClient:
            sshClient.close() 

def CaptureKeySpaceSnapshot(hostIP, username, password, keySpaces, backupPath=None):
    snapshotResults = {}
    
    try:
        sshClient = CreateSshClient(hostIP, 22, username, password)
        sftpClient = sshClient.open_sftp()
        for keySpace in keySpaces:
            command = f'nodetool snapshot -t {keySpace} {keySpace}'
            stdin, stdout, stderr = sshClient.exec_command(command)
            
            # Read stdout and stderr
            stdoutOutput = stdout.read().decode()
            errorOutput = stderr.read().decode()

            print(stdoutOutput)
            if errorOutput:
                print(errorOutput)

            snapshotIdMatch = re.search(r'snapshot name \[(\S+)\]', stdoutOutput)
            if snapshotIdMatch:
                snapshotId = snapshotIdMatch.group(1)
                print(f"Snapshot for keyspace '{keySpace}' taken successfully. Snapshot ID: {snapshotId}")
                
                basePath = f"/var/lib/scylla/data/{keySpace}/"
                
                # List all tables in the keyspace
                listTablesCommand = f'ls {basePath}'
                stdin, stdout, stderr = sshClient.exec_command(listTablesCommand)
                tablePaths = stdout.read().decode().splitlines()
                
                snapshotPaths = []
                localSnapshotPaths = []
                for tablePath in tablePaths:
                    tableUUIDMatch = re.search(r'-(\S+)', tablePath)
                    if tableUUIDMatch:
                        tableUUID = tableUUIDMatch.group(1)
                        
                    # Construct the path to the snapshot for each table
                    snapshotPath = f"{basePath}{tablePath}/snapshots/{snapshotId}/"
                    if CheckDirExists(sshClient, snapshotPath):
                        snapshotPaths.append((snapshotPath, tableUUID))
                        
                        if backupPath:
                            localTableBackupPath = os.path.join(backupPath, keySpace, tablePath, "snapshots", snapshotId)
                            os.makedirs(localTableBackupPath, exist_ok=True)

                            # Copy each file from the remote snapshot directory to the local machine
                            remoteFiles = sftpClient.listdir(snapshotPath)
                            for remoteFile in remoteFiles:
                                remoteFilePath = os.path.join(snapshotPath, remoteFile)
                                localFilePath = os.path.join(localTableBackupPath, remoteFile)
                                sftpClient.get(remoteFilePath, localFilePath)  # Transfer file
                                print(f"Transferred {remoteFilePath} to {localFilePath}")
                            localSnapshotPaths.append(localTableBackupPath)

                print(snapshotPaths)
                snapshotResults = {
                    'remote_paths': snapshotPaths,
                    'local_paths': localSnapshotPaths if backupPath else None
                }
            else:
                print("Error: Snapshot directory not found in the output.")
                snapshotResults = None
        
        return snapshotResults

    except Exception as e:
        print(f"Error taking remote snapshot: {e}")
        return None

    finally:
        sshClient.close()
        sftpClient.close()

def RestoreKeySpaceFromLocal(hostIP, username, password, keySpace, localSnapshotPaths):
    try:
        # Connect to the remote ScyllaDB server
        sshClient = CreateSshClient(hostIP, 22, username, password)

        for localPath in localSnapshotPaths:
            tableNameWithUUID = localPath.split(os.path.sep)[-3]
            # tableNameWithUUID = path_components[-3] 
            match = re.match(r'([^\-]+)-(.*)', tableNameWithUUID)
            tableName = match.group(1)
            tableId = GetTableUuid(hostIP, keySpace, tableName)
            uuid = str(tableId).replace("-", "")
            
            remoteTablePath = f"/var/lib/scylla/data/{keySpace}/{tableName}-{uuid}"
            tempRemotePath = f"/tmp/temp_scylla_data/{tableName}-{uuid}"
            stdin, stdout, stderr = sshClient.exec_command(f"mkdir -p {tempRemotePath}")
            CheckForErrors(stdout, stderr)
            snapshotPath = localPath
            
            with SCPClient(sshClient.get_transport()) as scp:
                # Copy files from local backup to the remote snapshot directory
                localFiles = os.listdir(snapshotPath)
                for localFile in localFiles:
                    localFilePath = os.path.join(snapshotPath, localFile)
                    tmpremoteFilePath = os.path.join(tempRemotePath, localFile)
                    try:
                        scp.put(localFilePath, tmpremoteFilePath)
                        print(f"Restored {localFilePath} to {tmpremoteFilePath}")
                    except Exception as e:
                        print(str(e))
            
            time.sleep(5)
                    
            command = f'echo {password} | sudo -S chown scylla:scylla {tempRemotePath}/*'
            stdin, stdout, stderr = sshClient.exec_command(command)
            CheckForErrors(stdout, stderr)
            
            command_move_files = f"echo {password} | sudo -S mv {tempRemotePath}/* {remoteTablePath}/"
            print(command_move_files)
            stdin, stdout, stderr = sshClient.exec_command(command_move_files)
            CheckForErrors(stdout, stderr)

            stderr_output = stderr.read().decode()
            if stderr_output:
                print("Error during moving files:", stderr_output)
            
            command = f'echo {password} | sudo -S rm -rf "{tempRemotePath}"'
            stdin, stdout, stderr = sshClient.exec_command(command)
            CheckForErrors(stdout, stderr)

            # # Check if files were moved successfully
            # command_list_dest_files = f"ls -l {remoteTablePath}"
            # stdin, stdout, stderr = sshClient.exec_command(command_list_dest_files)
            # print("Files in destination directory:")
            # print(stdout.read().decode())

        print(f"Restoration of keyspace '{keySpace}' from local snapshots completed.")
        return True

    except Exception as e:
        print(f"Error during restoration: {e}")
        return False

    finally:
        sshClient.close()


# def CreatNewKeyspace(host, username, password, keyspace):
#     try:
#         # Create an SSH client
#         sshClient = CreateSshClient(host, 22, username, password)
#         # Create the new keyspace
#         createKeyspaceCommand = f"cqlsh -e 'CREATE KEYSPACE {keyspace} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 3}};'"
#         sshClient.exec_command(createKeyspaceCommand)
#         print(f"Keyspace '{keyspace}' created successfully.")
#         return True

#     except Exception as e:
#         print(f"Error creating new keyspace: {e}")
#     finally:
#         sshClient.close()

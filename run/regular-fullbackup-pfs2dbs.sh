#!/bin/bash

# User should config the following params
src=""
gatewaylist=""
instanceid=""
hostlist=("xx.xx.xx.xx" "xx.xx.xx.xx" "xx.xx.xx.xx")
port=""
username=""
password=""
passwordbase64=""
database="postgres"
enableencryption="false"
encryptionpassword=""
workercount=16
compress="true"
maxfilesize=10737418240

# enter execute dir
cd /usr/local/polardb_o_backup_tool_current/bin

# find the rw endpoint
masterEndpoint=""
for host in ${hostlist[@]}
do
   result=$(./pgsqltool -host $host -port $port -user $username -pwd $password -db $database -cmd ismaster)
   if [ "$result" == "TRUE" ]
   then
      echo "$host is master"
      masterEndpoint=$host
      break
   fi
done

if [[ "$masterEndpoint" == "" ]]
then
    echo "[ERROR] not found master endpoint, please check the hostlist"
    exit
fi

date=$(date -d "-0 day" +%Y%m%d)
backupid="fbkid-$date"
loopname=$(echo $src | cut -d '/' -f 2)
dbpath=/$(echo $src | cut -d '/' -f 3)
./backup_ctl -instance "$instanceid-full" -plugin pgpipeline -dbs '{
    "InstanceID": "'$instanceid'",
    "GatewayList": ["'$gatewaylist'"],
    "BackupID": "'$backupid'",
    "Name": "dbs"
}' -pgpipeline '{
    "EnableEncryption": '$enableencryption',
    "EncryptionPassword": "'$encryptionpassword'",
    "Compress": '$compress',
    "WorkerCount": '$workercount',
    "Action": "backup",
    "MaxFileSize": '$maxfilesize',
    "HostIP": "127.0.0.1",
    "Name": "pgpipeline",
    "Force": false,
    "ManagerAddr": ["127.0.0.1:1888"],
    "InstanceID": "'$instanceid'",
    "BackupID": "'$backupid'",
    "Frontend": "pfs",
    "Backend":"dbs",
    "Endpoints": {
        "pfs": {
            "Type": "Frontend",
            "PluginName": "pfs",
            "InitFunc": "ExportMapping",
            "ConfPath": "pfs.conf"
        },
        "dbs": {
            "Type": "Backend",
            "PluginName": "dbs",
            "InitFunc": "ExportMapping",
            "ConfPath": "dbs.conf"
        }
    },
    "DBHomePath": "",
    "StatusURL": "/manager/BackupStatus",
    "PgDBConf": {
        "Endpoint": "'$masterEndpoint'",
        "Port": "'$port'",
        "Username": "'$username'",
        "Password": "'$passwordbase64'",
        "Database": "'$database'",
        "ApplicationName": "backup-agent"
    }
}' -pfs '{
    "Pbd": "'$loopname'",
    "Cluster": "disk",
    "Mode": "file",
    "Flags": "backup",
    "Name": "pfs",
    "DBPath": "'$dbpath'"
}'



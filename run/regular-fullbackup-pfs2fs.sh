#!/bin/bash -x

# User should config the following params
src=""
dst=""
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

date=$(date "+%Y%m%d-%H%M%S")
loopname=$(echo $src | cut -d '/' -f 2)
dbpath=/$(echo $src | cut -d '/' -f 3)
./backup_ctl -instance "$instanceid-full" -plugin pgpipeline -fs '{
    "Path": "'$dst'",
    "InstanceID": "'$instanceid'",
    "BackupID": "fbk-'$date'"
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
    "BackupID": "fbk-'$date'",
    "Frontend": "pfs",
    "Backend":"fs",
    "Endpoints": {
        "pfs": {
            "Type": "Frontend",
            "PluginName": "pfs",
            "InitFunc": "ExportMapping",
            "ConfPath": "pfs.conf"
        },
        "fs": {
            "Type": "Backend",
            "PluginName": "fs",
            "InitFunc": "ExportMapping",
            "ConfPath": "fs.conf"
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

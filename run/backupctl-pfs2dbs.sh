#!/bin/bash
let numparam=$#
if [[ $numparam != 9 ]]
then
    echo "[ERROR] params are not valid, usage: ./backupctl-pfs2fs.sh \$src \$gatewaylist \$instanceid \$backupid \$endpoint \$port \$username \$password \$database"
    exit
fi

echo "src: $1"
echo "dst: dbs"
loopname=$(echo $1 | cut -d '/' -f 2)
echo $loopname
dbpath=/$(echo $1 | cut -d '/' -f 3)
echo $dbpath
gatewaylist=$2
instanceid=$3
backupid=$4
endpoint=$5
port=$6
username=$7
password=$8
database=$9
sudo mkdir $path
sudo chmod 777 $path
./backup_ctl -plugin pgpipeline -dbs '{
    "InstanceID": "'$instanceid'",
    "GatewayList": ["'$gatewaylist'"],
    "BackupID": "'$backupid'",
    "Name": "dbs"
}' -pgpipeline '{
    "Compress": true,
    "WorkerCount": 2,
    "Action": "backup",
    "HostIP": "127.0.0.1",
    "Name": "pgpipeline",
    "Force": false,
    "ManagerAddr": ["127.0.0.1:1888"],
    "Endpoints": {
        "Frontend": {
            "Name": "Frontend",
            "PluginName": "pfs",
            "InitFunc": "ExportMapping",
            "ConfPath": "pfs.conf"
        },
        "Backend": {
            "Name": "Backend",
            "PluginName": "dbs",
            "InitFunc": "ExportMapping",
            "ConfPath": "dbs.conf"
        }
    },
    "DBHomePath": "",
    "StatusURL": "/manager/BackupStatus",
    "PgDBConf": {
        "Endpoint": "'$endpoint'",
        "Port": "'$port'",
        "Username": "'$username'",
        "Password": "'$password'",
        "Database": "'$database'",
        "ApplicationName": "none"
    }
}' -pfs '{
    "Pbd": "'$loopname'",
    "Cluster": "disk",
    "Mode": "file",
    "Flags": "backup",
    "Name": "pfs",
    "DBPath": "'$dbpath'"
}'


#!/bin/bash
let numparam=$#
if [[ $numparam != 4 ]]
then
    echo "[ERROR] params are not valid, usage: ./backupctl-dbs2pfs.sh \$dst \$gatewaylist \$instanceid \$backupid"
    exit
fi

echo "src: dbs"
echo "dst: $1"
loopname=$(echo $1 | cut -d '/' -f 2)
echo $loopname
dbpath=/$(echo $1 | cut -d '/' -f 3)
gatewaylist=$2
instanceid=$3
backupid=$4
echo $dbpath
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
    "Action": "restore",
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
    "StatusURL": "/manager/BackupStatus"
}' -pfs '{
    "Pbd": "'$loopname'",
    "Cluster": "disk",
    "Mode": "file",
    "Flags": "restore",
    "Name": "pfs",
    "DBPath": "'$dbpath'"
}'


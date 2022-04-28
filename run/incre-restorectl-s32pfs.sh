#!/bin/bash

# User should config the following params
wals='["000000020000000C00000001"]'

let numparam=$#
if [[ $numparam != 7 ]]
then
    echo "[ERROR] params are not valid, usage: ./full-restorectl-dbs2pfs.sh \$s3gateway \$accesskey \$secretkey \$bucket \$instanceid \$backupid \$dst"
    exit
fi

s3gateway=$1
accesskey=$2
secretkey=$3
bucket=$4
instanceid=$5
backupid=$6
devname=$(echo $7 | cut -d '/' -f 2)
dbpath=/$(echo $7 | cut -d '/' -f 3)

./backup_ctl -instance $instanceid -plugin increpipeline -increpipeline '{
    "Compress": true,
    "RealTime": false,
    "WorkerCount": 16,
    "Action": "restore",
    "HostIP": "127.0.0.1",
    "Name": "increpipeline",
    "Force": false,
    "ManagerAddr": ["127.0.0.1:1888"],
    "InstanceID": "'$instanceid'",
    "BackupID": "'$backupid'",
    "Frontend": "pfs",
    "Backend":"s3",
    "Endpoints": {
        "Frontend": {
            "Name": "Frontend",
            "PluginName": "pfs",
            "InitFunc": "ExportMapping",
            "ConfPath": "pfs.conf"
        },
        "Backend": {
            "Name": "Backend",
            "PluginName": "s3",
            "InitFunc": "ExportMapping",
            "ConfPath": "s3.conf"
        }
    },
    "RecoveryWals": '$wals',
    "DBHomePath": "",
    "StatusURL": "/manager/BackupStatus"
}' -pfs '{
    "Pbd": "'$devname'",
    "Cluster": "disk",
    "Mode": "file",
    "Flags": "restore",
    "Name": "pfs",
    "DBPath": "'$dbpath'"
}' -s3 '{
    "Endpoint": "'$s3gateway'",
    "ID": "'$accesskey'",
    "Key": "'$secretkey'",
    "Bucket": "'$bucket'",
    "InstanceID": "'$instanceid'",
    "BackupID": "'$backupid'",
    "Name": "s3"
}'
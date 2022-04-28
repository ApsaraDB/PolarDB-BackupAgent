#!/bin/bash
echo "src: $1"
echo "dst: $2"
InstanceID=$3
BackupID=$4
./backup_ctl -instance "'$InstanceID'" -plugin pgpipeline -fs1 '{
    "Path": "'$1'"
}' -fs2 '{
    "Path": "'$2'"
}' -pgpipeline '{
    "Compress": true,
    "WorkerCount": 8,
    "Action": "backup",
    "HostIP": "127.0.0.1",
    "Name": "pgpipeline",
    "Force": false,
    "Frontend": "fs1",
    "Backend":"fs2",
    "InstanceID":"'$InstanceID'",
    "BackupID":"'$BackupID'",
    "ManagerAddr": ["127.0.0.1:1888"],
    "Endpoints": {
        "fs1": {
            "Type": "Frontend",
            "PluginName": "fs1",
            "InitFunc": "ExportMapping",
            "ConfPath": "fs1.conf"
        },
        "fs2": {
            "Type": "Backend",
            "PluginName": "fs2",
            "InitFunc": "ExportMapping",
            "ConfPath": "fs2.conf"
        }
    },
    "DBHomePath": "",
    "StatusURL": "/manager/BackupStatus"
}'
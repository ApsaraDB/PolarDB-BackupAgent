#!/bin/bash
echo "src: $1"
echo "dst: $2"
./backup_ctl -plugin pgpipeline -fs1 '{
    "Path": "'$2'"
}' -fs2 '{
    "Path": "'$1'"
}' -pgpipeline '{
    "Compress": true,
    "WorkerCount": 2,
    "Action": "restore",
    "HostIP": "127.0.0.1",
    "Name": "pgpipeline",
    "Force": false,
    "ManagerAddr": "127.0.0.1:1888",
    "Endpoints": {
        "Frontend": {
            "Name": "Frontend",
            "PluginName": "fs1",
            "InitFunc": "ExportMapping",
            "ConfPath": "fs1.conf"
        },
        "Backend": {
            "Name": "Backend",
            "PluginName": "fs2",
            "InitFunc": "ExportMapping",
            "ConfPath": "fs2.conf"
        }
    },
    "DBHomePath": "",
    "StatusURL": "/manager/BackupStatus"
}'
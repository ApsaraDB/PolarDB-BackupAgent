#!/bin/bash
let numparam=$#
if [[ $numparam != 7 ]]
then
    echo "[ERROR] params are not valid, usage: ./backupctl-pfs2fs.sh \$src \$dst \$endpoint \$port \$username \$password \$database"
    exit
fi

echo "src: $1"
echo "dst: $2"
path=$2
echo $path
loopname=$(echo $1 | cut -d '/' -f 2)
echo $loopname
dbpath=/$(echo $1 | cut -d '/' -f 3)
echo $dbpath
endpoint=$3
port=$4
username=$5
password=$6
database=$7
sudo mkdir $path
sudo chmod 777 $path
./backup_ctl -plugin pgpipeline -fs '{
    "Path": "'$path'"
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
            "PluginName": "fs",
            "InitFunc": "ExportMapping",
            "ConfPath": "fs.conf"
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
#!/bin/bash -x

# User should config the following params
cmgateway=""
polardbdata=""
pgdata=""
dst=""
instanceid=""

# example
# cmgateway="xx.xx.xx.xx:5500"
# polardbdata="/mnt/disk2"
# pgdata="/var/local/polardb/clusters/mycluster"
# dst="xx.xx.xx.xx:8080"
# instanceid="polar-dma-test"

##########################################

# enter execute dir
cd /usr/local/polardb_o_backup_tool_current/bin

# generate backupid
date=$(date "+%Y%m%d-%H%M%S")
backupid="fbkid-$date"

# execute backup
curl "http://$cmgateway/manager/StartBackup" -d '{
  "InstanceID": "'$instanceid'",
  "BackupID": "'$backupid'",
  "BackupType": "Full",
  "BackupJobID": "default",
  "CallbackURL": "http://127.0.0.1:1889",
  "Filesystem":"fs",
  "BackupStorageSpace": {"StorageType":"http", "Locations":{"http": {"Endpoint": "'$dst'"}}},
  "BackupFolder": "'$polardbdata'",
  "DBClusterMetaDir":"'$pgdata'"
}'
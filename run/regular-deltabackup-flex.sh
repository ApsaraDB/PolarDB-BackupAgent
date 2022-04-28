#!/bin/bash -x

# User should config the following params
cmgateway=""
polardbdata=""
pgdata=""
storagetype=""
dst=""
instanceid=""

# example
# cmgateway="xx.xx.xx.xx:5500"
# polardbdata="/mnt/disk2"
# pgdata="/var/local/polardb/clusters/mycluster"
# storagetype="http"
# dst="xx.xx.xx.xx:8080"
# instanceid="polar-dma-test"

##########################################

# generate backupid
date=$(date "+%Y%m%d-%H%M%S")
backupid="bbkid-$date"

# execute backup
curl "http://$cmgateway/manager/StartBackup" -d '{
  "InstanceID": "'$instanceid'",
  "BackupID": "'$backupid'",
  "BackupType": "Full",
  "UseBlock": true,
  "BackupJobID": "default",
  "Filesystem":"fs",
  "BackupStorageSpace": {"StorageType":"'$storagetype'", "Locations":{"http": {"Endpoint": "'$dst'"}, "Local": {"Path": "'$dst'"}}},
  "BackupFolder": "'$polardbdata'",
  "DBClusterMetaDir":"'$pgdata'"
}'
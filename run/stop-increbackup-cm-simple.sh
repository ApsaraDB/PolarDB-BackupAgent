#!/bin/bash -x

# User should config the following params
cmgateway=""
instanceid=""

# example
# cmgateway="xx.xx.xx.xx:5500"
# instanceid="polar-dma-test"

##########################################

# enter execute dir
cd /usr/local/polardb_o_backup_tool_current/bin

# execute stop backup
curl "http://$cmgateway/manager/StopBackup" -d '{
  "InstanceID": "'$instanceid'",
  "BackupID": "increbk",
  "BackupType": "Incremental",
  "BackupJobID": "default"
}'
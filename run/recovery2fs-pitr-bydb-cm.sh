#!/bin/bash -x

# User should config the following params
cmgateway=""
backupmachine=""
polardbdata=""
dst=""
instanceid=""
increbackupid=""
hostlist=""
port=""
username=""
passwordbase64=""
database=""
recoverytime=

# example
# cmgateway="xx.xx.xx.xx:5500"
# backupmachine="xx.xx.xx.xx:1888"
# polardbdata="/mnt/disk3"
# dst="/root/testbk"
# instanceid="polar-dma-test"
# increbackupid="testincrebk"
# hostlist="xx.xx.xx.xx,xx.xx.xx.xx,xx.xx.xx.xx"
# port="1521"
# username="user_rep"
# passwordbase64="cGdzcWwK"
# database="postgres"
# recoverytime=1616050150

##########################################

curl "http://$cmgateway/manager/Recovery" -d '{
  "GatewayAddressList": [""],
  "BackupMachineList": ["http://'$backupmachine'"],
  "InstanceID": "'$instanceid'",
  "BackupJobID": "default",
  "CallbackURL": "http://127.0.0.1:1889",
  "BackupMetaSource": "db",
  "RecoveryTime": '$recoverytime',
  "Full": {"InstanceID": "'$instanceid'"},
  "Incremental": {"InstanceID": "'$instanceid'", "BackupID": "'$increbackupid'"},
  "Filesystem":"fs",
  "BackupAccount": {"User": "'$username'","Password": "'$passwordbase64'","Endpoint": "'$hostlist'","Port": "'$port'"},
  "RecoveryFolder": "'$polardbdata'"
}'
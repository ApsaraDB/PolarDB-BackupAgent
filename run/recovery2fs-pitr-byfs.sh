#!/bin/bash
backupmachine=""
polardbdata=""
dst=""
instanceid=""
fullbackupid=""
recoverymode=""
recoverytime=

cd /usr/local/polardb_o_backup_tool_current/bin

echo "clear previous status"
rm -f icrefinish
rm -f fullfinish
rm -f error

echo "start program"
nohup ./backup_ctl > backup_ctl.log 2>&1& echo $! > backup_ctl.pid
sleep 2s

echo "curl program"
rescode=$(curl "http://127.0.0.1:1888/manager/Recovery" -d '{
  "GatewayAddressList": [""],
  "BackupMachineList": ["http://'$backupmachine'"],
  "InstanceID": "'$instanceid'",
  "BackupJobID": "default",
  "CallbackURL": "http://127.0.0.1:1889",
  "BackupMetaSource": "fs",
  "RecoveryTime": '$recoverytime',
  "Full": {"InstanceID": "'$instanceid'", "BackupID": "'$fullbackupid'"},
  "Incremental": {"InstanceID": "'$instanceid'", "BackupID": "increbk"},
  "Filesystem":"fs",
  "RecoveryFolder": "'$polardbdata'",
  "BackupStorageSpace": {"StorageType":"fs", "Locations":{"Local": {"Path": "'$dst'"}}}
}')
echo "curl done"
echo $rescode
if [[ $rescode != '{"error": null, "code": 0}' ]]
then
    kill `cat backup_ctl.pid`
    echo -e "\nrecovery failed"
    exit 1
fi

ffile="icrefinish"
if [[ $recoverymode == 'full' ]]
then
    ffile="fullfinish"
fi
efile="error"
echo -e "\nrecovering...\c"
while :
do
    if [ -e $ffile ];then
        kill `cat backup_ctl.pid`
        echo -e "\nrecovery finish"
        break 2
    fi
    if [ -e $efile ];then
        kill `cat backup_ctl.pid`
        echo "\nrecovery failed"
        break 2
    fi
    echo -e ".\c"
    sleep 1s
done
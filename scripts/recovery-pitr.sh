#!/bin/bash
let numparam=$#
if [[ $numparam != 10 ]]
then
    echo "[ERROR] params are not valid, usage: ./recovery-pitr.sh \$src \$dst \$instanceid \$endpoint \$port \$username \$password \$database \$recoverytime \$recoverymode"
    exit 1
fi

path=$1
loopname=$(echo $2 | cut -d '/' -f 2)
dbpath=/$(echo $2 | cut -d '/' -f 3)
instanceid=$3
endpoint=$4
port=$5
username=$6
password=$7
database=$8
recoverytime=$9
recoverymode=${10}

echo "check params"
if [[ $recoverymode != 'full' && $recoverymode != 'pitr' ]]
then
    echo -e "\n[ERROR] Not support recovery mode"
    exit 1
fi

echo "clear previous status"
rm -f icrefinish
rm -f fullfinish
rm -f error

echo "set daemon mode"
sed -i 's/"Enable": false/"Enable": true/g' loader.conf

echo "start program"
nohup ./backup_ctl > backup_ctl.log 2>&1& echo $! > backup_ctl.pid
sleep 2s

# reset mode
sed -i 's/"Enable": true/"Enable": false/g' loader.conf

echo "curl program"
rescode=$(curl "http://127.0.0.1:1888/manager/Recovery" -d '{
  "GatewayAddressList": ["127.0.0.1:9999"],
  "BackupMachineList": ["http://127.0.0.1:1888"],
  "BackupJobID": "default",
  "StartOffset": 0,
  "CallbackURL": "http://127.0.0.1:1889",
  "BackupPBD": "'$loopname'",
  "InstanceID": "'$instanceid'",
  "RecoveryTime": '$recoverytime',
  "RecoveryMode": "'$recoverymode'",
  "BackupAccount": {"User": "'$username'","Password": "'$password'","Endpoint": "'$endpoint'","Port": "'$port'","Database": "'$database'"},
  "Filesystem":"pfs.file",
  "BackupStorageSpace": {"StorageType":"fs", "Locations":{"Local": {"Path": "'$path'"}}}
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

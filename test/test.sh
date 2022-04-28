# check pwd
password=polardb1234
if [ $password == notdefine ]
then
 echo 'please config password in test.sh first'
 exit 1
fi
# build test dir
echo 'build test dir ...'
mkdir test
cd test
echo 'build test dir'
# dd image
echo 'dd image ...'
dd if=/dev/zero of=test20gb.img bs=40960 count=524288
echo 'dd image done!'
# mount loop dev
echo 'mount loop dev ...'
findloop=$(sudo losetup -f)
echo $password | sudo -S losetup $findloop test20gb.img
echo 'mount loop dev done!'
# mkfs
echo 'mkfs ...'
loopname=$(echo $findloop | cut -d '/' -f 3)
echo $password | sudo -S pfs -C disk mkfs -f $loopname
echo $password | sudo -S pfs -C disk mkdir /$loopname/data
echo $password | sudo -S pfs -C disk mkdir /$loopname/data/testdir
echo $password | sudo -S pfs -C disk mkdir /$loopname/data/global
echo $password | sudo -S mv ../testfile ../pg_control
echo $password | sudo -S cat ../pg_control | pfs -C disk write /$loopname/data/global/pg_control
mv ../pg_control ../testfile
echo $password | sudo -S pfs -C disk touch /$loopname/data/testdir/testfile
dd if=/dev/zero of=test1gb.img bs=4096 count=262144
echo $password | sudo -S cat test1gb.img | pfs -C disk write /$loopname/data/testdir/testfile
echo $password | sudo -S pfs -C disk ls /$loopname/data/testdir/
echo 'mkfs done!'
# install database & start pg

# prepare backup dir
echo 'prepare backup dir ...'
echo $password | sudo -S mkdir /backup_data_tmp
echo $password | sudo -S chmod 777 /backup_data_tmp
echo $password | sudo -S chmod 777 $findloop
echo 'prepare backup dir done!'
# backup
echo 'backup ...'
cd ..
./backup_ctl -plugin pgpipeline -fs '{
    "Path": "/backup_data_tmp"
}' -pgpipeline '{
    "Compress": true,
    "WorkerCount": 2,
    "Action": "backup",
    "HostIP": "127.0.0.1",
    "Name": "pgpipeline",
    "Force": false,
    "ManagerAddr": "127.0.0.1:1888",
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
    "StatusURL": "/manager/BackupStatus"
}' -pfs '{
    "Pbd": "'$loopname'",
    "Cluster": "disk",
    "Mode": "file",
    "Flags": "backup",
    "Name": "pfs",
    "DBPath": "/data"
}'
echo 'backup done!'
# delete pfs data
echo 'delete pfs data ...'
echo $password | sudo -S pfs -C disk rm -r /$loopname/data
echo $password | sudo -S pfs -C disk ls /$loopname/
echo $password | sudo -S pfs -C disk mkdir /$loopname/data
echo 'delete pfs data done!'
# restore
echo 'restore ...'
./backup_ctl -plugin pgpipeline -fs '{
    "Path": "/backup_data_tmp"
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
    "StatusURL": "/manager/BackupStatus"
}' -pfs '{
    "Pbd": "'$loopname'",
    "Cluster": "disk",
    "Mode": "file",
    "Flags": "restore",
    "Name": "pfs",
    "DBPath": "/data"
}'
echo 'restore done!'
# check restore
echo 'check restore ...'
count=$(echo $password | sudo -S pfs -C disk ls /$loopname/data/testdir | grep -c 'failed')
echo $password | sudo -S pfs -C disk ls /$loopname/data/testdir
echo 'check restore done!'
# start pg

# umount loop dev
echo 'umount loop dev ...'
sudo losetup -d $findloop
echo 'umount loop dev done!'
# remove backup data
echo 'remove backup data ...'
sudo rm -r /backup_data_tmp
echo 'remove backup data done!'
# remove test dir
echo 'remove test dir ...'
sudo rm -r test
echo 'remove test dir done!'
if [ $count == 0 ]
then
 echo "Test succesfully!"
else
 echo "Test failed!"
fi

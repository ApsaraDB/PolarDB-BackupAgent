#!/bin/bash

# case 32: 
# action: do full backup using script -> insert some data -> HA -> insert some data on new rw -> do block backup using script -> recovery through full backup and block backup/ wals using script
# enviroment: test rw node for flex
# prepare: http_file_server should be running
# interface: cm manager

# config some param for test
Instance="test-flex-ins"
httpaddr="47.118.41.173:8080"
testnode="47.118.56.58"
newrwnode="118.178.122.235"
pgdata_r="/var/local/polardb/clusters/mycluster_r"
polardata_r="/mnt/polardb_cluster_mycluster_r"

echo '########## restart backup ##########'
echo Ali165243 | sudo -S sed -i "s/\"EnableBlockBackup\": false/\"EnableBlockBackup\": true/g" /usr/local/polardb_o_backup_tool_current/bin/pgpipeline.conf
echo Ali165243 | sudo -S sed -i "s/\"EnableBlockBackup\": false/\"EnableBlockBackup\": true/g" /usr/local/polardb_o_backup_tool_current/bin/increpipeline.conf
echo Ali165243 | sudo -S ssh root@118.178.122.235 'sed -i "s/\"EnableBlockBackup\": false/\"EnableBlockBackup\": true/g" /usr/local/polardb_o_backup_tool_current/bin/pgpipeline.conf'
echo Ali165243 | sudo -S ssh root@118.178.122.235 'sed -i "s/\"EnableBlockBackup\": false/\"EnableBlockBackup\": true/g" /usr/local/polardb_o_backup_tool_current/bin/increpipeline.conf'
echo Ali165243 | sudo -S systemctl restart backupctl.service
echo Ali165243 | sudo -S ssh root@118.178.122.235 'systemctl restart backupctl.service'
sleep 1
echo Ali165243 | sudo -S sed -i "s/\"EnableBlockBackup\": true/\"EnableBlockBackup\": false/g" /usr/local/polardb_o_backup_tool_current/bin/pgpipeline.conf
echo Ali165243 | sudo -S sed -i "s/\"EnableBlockBackup\": true/\"EnableBlockBackup\": false/g" /usr/local/polardb_o_backup_tool_current/bin/increpipeline.conf
echo Ali165243 | sudo -S ssh root@118.178.122.235 'sed -i "s/\"EnableBlockBackup\": true/\"EnableBlockBackup\": false/g" /usr/local/polardb_o_backup_tool_current/bin/pgpipeline.conf'
echo Ali165243 | sudo -S ssh root@118.178.122.235 'sed -i "s/\"EnableBlockBackup\": true/\"EnableBlockBackup\": false/g" /usr/local/polardb_o_backup_tool_current/bin/increpipeline.conf'
echo 'restart backup service'
echo ""
read -p "enter to continue" 

echo '########## stop test db ##########'
pg_ctl stop -D /var/local/polardb/clusters/mycluster_r
echo 'stop test db whose data is /var/local/polardb/clusters/mycluster_r'
echo ""
read -p "enter to continue" 

echo '########## prepare origin data ##########'
rm -rf ~/test.sql
touch ~/test.sql
echo 'drop table employees;' >> ~/test.sql
cat ~/test.sql
psql -h 127.0.0.1 -p 1521 -d polardb -U polardb -f ~/test.sql > ~/genlogfile
sleep 1
rm ~/test.sql
echo 'excute sql done!'
cat ~/genlogfile
rm ~/genlogfile
echo ""
read -p "enter to continue" 

echo '########## start wal backup ##########'
script="/usr/local/polardb_o_backup_tool_current/bin/regular-increbackup-flex.sh"
echo Ali165243 | sudo -S sed -i "s/cmgateway=\"\"/cmgateway=\"47.118.56.58:5500\"/g" $script
echo Ali165243 | sudo -S sed -i "s/polardbdata=\"\"/polardbdata=\"\/mnt\/polardb_cluster_mycluster\"/g" $script
echo Ali165243 | sudo -S sed -i "s/pgdata=\"\"/pgdata=\"\/var\/local\/polardb\/clusters\/mycluster\"/g" $script
echo Ali165243 | sudo -S sed -i "s/storagetype=\"\"/storagetype=\"http\"/g" $script
echo Ali165243 | sudo -S sed -i "s/dst=\"\"/dst=\"47.118.41.173:8080\"/g" $script
echo Ali165243 | sudo -S sed -i "s/instanceid=\"\"/instanceid=\"$Instance\"/g" $script
# cat $script
bash $script
echo Ali165243 | sudo -S sed -i "s/cmgateway=\"47.118.56.58:5500\"/cmgateway=\"\"/g" $script
echo Ali165243 | sudo -S sed -i "s/polardbdata=\"\/mnt\/polardb_cluster_mycluster\"/polardbdata=\"\"/g" $script
echo Ali165243 | sudo -S sed -i "s/pgdata=\"\/var\/local\/polardb\/clusters\/mycluster\"/pgdata=\"\"/g" $script
echo Ali165243 | sudo -S sed -i "s/storagetype=\"http\"/storagetype=\"\"/g" $script
echo Ali165243 | sudo -S sed -i "s/dst=\"47.118.41.173:8080\"/dst=\"\"/g" $script
echo Ali165243 | sudo -S sed -i "s/instanceid=\"$Instance\"/instanceid=\"\"/g" $script
sleep 2
echo ""
read -p "enter to continue" 

echo '########## start full backup ##########'
script="/usr/local/polardb_o_backup_tool_current/bin/regular-fullbackup-flex.sh"
echo Ali165243 | sudo -S sed -i "s/cmgateway=\"\"/cmgateway=\"47.118.56.58:5500\"/g" $script
echo Ali165243 | sudo -S sed -i "s/polardbdata=\"\"/polardbdata=\"\/mnt\/polardb_cluster_mycluster\"/g" $script
echo Ali165243 | sudo -S sed -i "s/pgdata=\"\"/pgdata=\"\/var\/local\/polardb\/clusters\/mycluster\"/g" $script
echo Ali165243 | sudo -S sed -i "s/storagetype=\"\"/storagetype=\"http\"/g" $script
echo Ali165243 | sudo -S sed -i "s/dst=\"\"/dst=\"47.118.41.173:8080\"/g" $script
echo Ali165243 | sudo -S sed -i "s/instanceid=\"\"/instanceid=\"$Instance\"/g" $script
# cat $script
bash $script
echo Ali165243 | sudo -S sed -i "s/cmgateway=\"47.118.56.58:5500\"/cmgateway=\"\"/g" $script
echo Ali165243 | sudo -S sed -i "s/polardbdata=\"\/mnt\/polardb_cluster_mycluster\"/polardbdata=\"\"/g" $script
echo Ali165243 | sudo -S sed -i "s/pgdata=\"\/var\/local\/polardb\/clusters\/mycluster\"/pgdata=\"\"/g" $script
echo Ali165243 | sudo -S sed -i "s/storagetype=\"http\"/storagetype=\"\"/g" $script
echo Ali165243 | sudo -S sed -i "s/dst=\"47.118.41.173:8080\"/dst=\"\"/g" $script
echo Ali165243 | sudo -S sed -i "s/instanceid=\"$Instance\"/instanceid=\"\"/g" $script
echo ""
read -p "enter to continue" 

echo '########## make some change ##########'
rm -rf ~/test.sql
touch ~/test.sql
echo 'create table employees (id int primary key not null, name text, age int, addr text, salary int);' >> ~/test.sql
echo 'insert into employees SELECT generate_series(0,1000) as key,repeat( chr(int4(random()*26)+65),4), (random()*(6^2))::integer,null,(random()*(10^4))::integer;
' >> ~/test.sql
cat ~/test.sql
psql -h 127.0.0.1 -p 1521 -d polardb -U polardb -f ~/test.sql > ~/genlogfile
sleep 1
rm -rf ~/test.sql
echo 'excute sql done!'
cat ~/genlogfile
rm ~/genlogfile
echo ""
read -p "enter to continue" 

echo '########## HA ##########'
echo 'ha and insert data in new rw please'
pg_ctl restart -D /var/local/polardb/clusters/mycluster
sleep 20
echo 'ha done, now 118.178.122.235 is new rw'
echo ""
read -p "enter to continue" 

echo '########## insert some data on new rw ##########'
echo Ali165243 | sudo -S ssh root@118.178.122.235 'su - polardb -c "rm -rf /home/polardb/test.sql"'
echo Ali165243 | sudo -S ssh root@118.178.122.235 'su - polardb -c "touch /home/polardb/test.sql"'
echo Ali165243 | sudo -S ssh root@118.178.122.235 'su - polardb -c "echo insert into employees SELECT generate_series\(1001,2000\) as key\,repeat\( chr\(int4\(random\(\)\*26\)\+65\)\,4\)\, \(random\(\)\*\(6\^2\)\)\:\:integer\,null\,\(random\(\)\*\(10^4\)\)\:\:integer\; >> /home/polardb/test.sql"'
echo Ali165243 | sudo -S ssh root@118.178.122.235 'su - polardb -c "psql -h 127.0.0.1 -p 1521 -d polardb -U polardb -f /home/polardb/test.sql > /home/polardb/genlogfile"'
echo Ali165243 | sudo -S ssh root@118.178.122.235 'su - polardb -c "cat /home/polardb/genlogfile"'
echo Ali165243 | sudo -S ssh root@118.178.122.235 'su - polardb -c "rm /home/polardb/genlogfile"'
echo ""
read -p "enter to continue" 

echo '########## start block backup ##########'
script="\/usr\/local\/polardb_o_backup_tool_current\/bin\/regular-deltabackup-flex.sh"

echo Ali165243 | sudo -S ssh root@118.178.122.235 'sed -i "s/cmgateway=\"\"/cmgateway=\"47.118.56.58:5500\"/g" '$script''
echo Ali165243 | sudo -S ssh root@118.178.122.235 'sed -i "s/polardbdata=\"\"/polardbdata=\"\/mnt\/polardb_cluster_mycluster\"/g" '$script''
echo Ali165243 | sudo -S ssh root@118.178.122.235 'sed -i "s/pgdata=\"\"/pgdata=\"\/var\/local\/polardb\/clusters\/mycluster\"/g" '$script''
echo Ali165243 | sudo -S ssh root@118.178.122.235 'sed -i "s/storagetype=\"\"/storagetype=\"http\"/g" '$script''
echo Ali165243 | sudo -S ssh root@118.178.122.235 'sed -i "s/dst=\"\"/dst=\"47.118.41.173:8080\"/g" '$script''
echo Ali165243 | sudo -S ssh root@118.178.122.235 'sed -i "s/instanceid=\"\"/instanceid=\"'$Instance'\"/g" '$script''

# cat $script
echo Ali165243 | sudo -S ssh root@118.178.122.235 bash $script

echo Ali165243 | sudo -S ssh root@118.178.122.235 'sed -i "s/cmgateway=\"47.118.56.58:5500\"/cmgateway=\"\"/g" '$script''
echo Ali165243 | sudo -S ssh root@118.178.122.235 'sed -i "s/polardbdata=\"\/mnt\/polardb_cluster_mycluster\"/polardbdata=\"\"/g" '$script''
echo Ali165243 | sudo -S ssh root@118.178.122.235 'sed -i "s/pgdata=\"\/var\/local\/polardb\/clusters\/mycluster\"/pgdata=\"\"/g" '$script''
echo Ali165243 | sudo -S ssh root@118.178.122.235 'sed -i "s/storagetype=\"http\"/storagetype=\"\"/g" '$script''
echo Ali165243 | sudo -S ssh root@118.178.122.235 'sed -i "s/dst=\"47.118.41.173:8080\"/dst=\"\"/g" '$script''
echo Ali165243 | sudo -S ssh root@118.178.122.235 'sed -i "s/instanceid=\"'$Instance'\"/instanceid=\"\"/g" '$script''
echo ""
read -p "enter to continue" 

echo '########## prepare recovery folder ##########'
echo Ali165243 | sudo -S mkdir /mnt/polardb_cluster_mycluster_r
echo Ali165243 | sudo -S rm -rf /mnt/polardb_cluster_mycluster_r/*
echo Ali165243 | sudo -S mkdir /var/local/polardb/clusters/mycluster_r
echo Ali165243 | sudo -S rm -rf /var/local/polardb/clusters/mycluster_r/*
echo ""
read -p "enter to continue" 

echo '########## recovery full and block and wals ##########'
script="/usr/local/polardb_o_backup_tool_current/bin/recovery2fs-flex.sh"
echo Ali165243 | sudo -S sed -i "s/cmgateway=\"\"/cmgateway=\"47.118.56.58:5500\"/g" $script
echo Ali165243 | sudo -S sed -i "s/polardbdata=\"\"/polardbdata=\"\/mnt\/polardb_cluster_mycluster_r\"/g" $script
echo Ali165243 | sudo -S sed -i "s/pgdata=\"\"/pgdata=\"\/var\/local\/polardb\/clusters\/mycluster_r\"/g" $script
echo Ali165243 | sudo -S sed -i "s/storagetype=\"\"/storagetype=\"http\"/g" $script
echo Ali165243 | sudo -S sed -i "s/dst=\"\"/dst=\"47.118.41.173:8080\"/g" $script
echo Ali165243 | sudo -S sed -i "s/instanceid=\"\"/instanceid=\"$Instance\"/g" $script
echo Ali165243 | sudo -S sed -i "s/recoverytime=/recoverytime=2629276251/g" $script
echo Ali165243 | sudo -S sed -i "s/backupmachine=\"\"/backupmachine=\"$testnode:1888\"/g" $script
# cat $script
bash $script
echo Ali165243 | sudo -S sed -i "s/cmgateway=\"47.118.56.58:5500\"/cmgateway=\"\"/g" $script
echo Ali165243 | sudo -S sed -i "s/polardbdata=\"\/mnt\/polardb_cluster_mycluster_r\"/polardbdata=\"\"/g" $script
echo Ali165243 | sudo -S sed -i "s/pgdata=\"\/var\/local\/polardb\/clusters\/mycluster_r\"/pgdata=\"\"/g" $script
echo Ali165243 | sudo -S sed -i "s/storagetype=\"http\"/storagetype=\"\"/g" $script
echo Ali165243 | sudo -S sed -i "s/dst=\"47.118.41.173:8080\"/dst=\"\"/g" $script
echo Ali165243 | sudo -S sed -i "s/instanceid=\"$Instance\"/instanceid=\"\"/g" $script
echo Ali165243 | sudo -S sed -i "s/recoverytime=2629276251/recoverytime=/g" $script
echo Ali165243 | sudo -S sed -i "s/backupmachine=\"$testnode:1888\"/backupmachine=\"\"/g" $script
echo ""
read -p "enter to continue" 

echo '########## change permission of folders ##########'
echo Ali165243 | sudo -S chown polardb:polardb -R /mnt/polardb_cluster_mycluster_r
echo Ali165243 | sudo -S chmod 0700 -R /mnt/polardb_cluster_mycluster_r
echo Ali165243 | sudo -S chown polardb:polardb -R /var/local/polardb/clusters/mycluster_r
echo Ali165243 | sudo -S chmod 0700 -R /var/local/polardb/clusters/mycluster_r

echo '########## reset conf ##########'
rm -rf $pgdata_r/postmaster.pid
rm -rf $pgdata_r/polar_node_static.conf
sed -i "s/1521/1522/g" $pgdata_r/postgresql.conf
sed -i "s/polardb_cluster_mycluster/polardb_cluster_mycluster_r/g" $pgdata_r/postgresql.conf
cp $polardata_r/polar_exclusive_backup_label $pgdata_r/backup_label
rm -rf $pgdata_r/recovery.conf
touch $pgdata_r/recovery.conf
echo "recovery_target_time = '2022-09-16 16:01:40'" >> $pgdata_r/recovery.conf
echo "restore_command = 'echo not copy'" >> $pgdata_r/recovery.conf
cat $pgdata_r/recovery.conf

echo '########## stop pg ##########'
echo "stop the rw/ro"
pg_ctl stop -D /var/local/polardb/clusters/mycluster
echo "stop ro done"
echo Ali165243 | sudo -S ssh root@118.178.122.235 'su - polardb -c "pg_ctl stop -D /var/local/polardb/clusters/mycluster"'
echo "stop rw done"
echo ""
read -p "enter to continue" 

echo '########## start pg ##########'
sed -i "s/POLAR_CLUSTER_ID=mycluster/POLAR_CLUSTER_ID=mycluster_r/g" ~/.bashrc
source ~/.bashrc
echo Ali123456 | su - root -c "cd /root/pdbcli-0.4.0; pdbcli create cluster --create-from-backup --config=/root/pdbcli-0.4.0/config.rec.yaml"
sed -i "s/POLAR_CLUSTER_ID=mycluster_r/POLAR_CLUSTER_ID=mycluster/g" ~/.bashrc
source ~/.bashrc
echo ""
read -p "enter to continue" 

echo '########## check pg data ##########'
echo "sleep 20 s for recovery"
sleep 20
echo "awake"
rm -rf ~/test.sql
touch ~/test.sql
echo 'select count(*) from employees;' >> ~/test.sql
cat ~/test.sql
psql -h 127.0.0.1 -p 1522 -d polardb -U polardb -f ~/test.sql > ~/genlogfile
sleep 1
rm -rf ~/test.sql
echo 'excute sql done, output should be 2001!'
cat ~/genlogfile
rm ~/genlogfile
echo ""
read -p "enter to continue" 

echo '########## recovery test db enviroment ##########'
echo "stop recovery db"
echo Ali165243 | sudo -S systemctl stop polardb-mycluster_r.service
echo "start rw"
pg_ctl start -D /var/local/polardb/clusters/mycluster
sleep 5
echo "start ro"
echo Ali165243 | sudo -S ssh root@118.178.122.235 'su - polardb -c "pg_ctl start -D /var/local/polardb/clusters/mycluster > /dev/null"'
echo "stop recovery db"
pg_ctl stop -D /var/local/polardb/clusters/mycluster_r
echo "test done!"
echo ""
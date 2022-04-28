Summary: Backup Agent package.
Name: t-polardb-backupagent
Version: 1.0.0
Release: RELEASEDATE.alios7
License: GPL
Group:Development/Tools
%description
PolarDB backup agent.
 
%define SERVICE_NAME backupctl.service
%define SERVICE_FILE /usr/lib/systemd/system/backupctl.service

%prep
%build
 
%install  # 安装阶段
mkdir -p $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
mkdir -p $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/log
mkdir -p $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/script/dbstack
cd $RPM_BUILD_DIR
pwd
cp ../BUILD/backup_ctl $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/pgsqltool $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/recoverytool $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/metatool $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/http_file_server $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/manager.so $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/pgpipeline.so $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/pfs.so $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/fs.so $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/increpipeline.so $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/minio.so $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/oss.so $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/pipeline.so $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/wal.so $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/http.so $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/dbs.conf $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/fs1.conf $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/fs2.conf $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/fs.conf $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/fspg.conf $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/fsbk.conf $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/fs_local_backend.conf $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/http.conf $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/increpipeline.conf $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/loader.conf $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/localfs.conf $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/manager.conf $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/pfs.conf $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/pgpipeline.conf $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/pipeline.conf $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/s3.conf $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/wal.conf $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/backupctl-fs2fs.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/backupctl-multifs2fs.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/download-dbs2fs.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/full-restorectl-dbs2pfs.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/full-restorectl-fs2pfs.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/full-restorectl-s32pfs.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/incre-restorectl-dbs2pfs.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/incre-restorectl-fs2pfs.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/incre-restorectl-s32pfs.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/recovery2fs-pitr-bydb.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/recovery2fs-pitr-byfs.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/recovery-pitr.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/regular-fullbackup-fs2fs.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/regular-fullbackup-pfs2dbs.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/regular-fullbackup-pfs2fs.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/regular-fullbackup-pfs2s3.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/regular-increbackup-fs2fs.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/regular-increbackup-pfs2dbs.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/regular-increbackup-pfs2fs.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/regular-increbackup-pfs2s3.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/restorectl-fs2fs.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/restore-initdb.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/backupctl-multifs2fs-cm.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/backupctl-multifs2http-cm.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/recovery2fs-pitr-bydb-cm.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/recovery2fs-pitr-byfs-cm.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/recovery2fs-pitr-byhttp-cm.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/regular-increbackup-fs2fs-cm.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/regular-increbackup-fs2http-cm.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/stop-increbackup-cm.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/install-httpfiled.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/regular-fullbackup-fs2dbs.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/regular-increbackup-fs2dbs.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/recovery-dbs2fs.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/regular-fullbackup-flex.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/regular-increbackup-flex.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/stop-increbackup-flex.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/regular-deltabackup-flex.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/recovery2fs-flex.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/VERSION $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/CHANGELOG.md $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/script/dbstack/* $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/script/dbstack
cp ../BUILD/backupgc $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/backupgc.conf $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/install-backupgcd.sh $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/pbkcli $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin
cp ../BUILD/sample.backup.conf $RPM_BUILD_ROOT/usr/local/polardb_backup_agent_current/bin

#rpm 包含文件
%files
/usr/local/polardb_backup_agent_current/bin/backup_ctl
/usr/local/polardb_backup_agent_current/bin/pgsqltool
/usr/local/polardb_backup_agent_current/bin/recoverytool
/usr/local/polardb_backup_agent_current/bin/metatool
/usr/local/polardb_backup_agent_current/bin/http_file_server
/usr/local/polardb_backup_agent_current/bin/manager.so
/usr/local/polardb_backup_agent_current/bin/pfs.so
/usr/local/polardb_backup_agent_current/bin/pgpipeline.so
/usr/local/polardb_backup_agent_current/bin/fs.so
/usr/local/polardb_backup_agent_current/bin/increpipeline.so
/usr/local/polardb_backup_agent_current/bin/pipeline.so
/usr/local/polardb_backup_agent_current/bin/minio.so
/usr/local/polardb_backup_agent_current/bin/oss.so
/usr/local/polardb_backup_agent_current/bin/wal.so
/usr/local/polardb_backup_agent_current/bin/http.so
/usr/local/polardb_backup_agent_current/bin/dbs.conf
/usr/local/polardb_backup_agent_current/bin/fs1.conf
/usr/local/polardb_backup_agent_current/bin/fs2.conf
/usr/local/polardb_backup_agent_current/bin/fs.conf
/usr/local/polardb_backup_agent_current/bin/fspg.conf
/usr/local/polardb_backup_agent_current/bin/fsbk.conf
/usr/local/polardb_backup_agent_current/bin/fs_local_backend.conf
/usr/local/polardb_backup_agent_current/bin/http.conf
/usr/local/polardb_backup_agent_current/bin/increpipeline.conf
/usr/local/polardb_backup_agent_current/bin/loader.conf
/usr/local/polardb_backup_agent_current/bin/localfs.conf
/usr/local/polardb_backup_agent_current/bin/manager.conf
/usr/local/polardb_backup_agent_current/bin/pfs.conf
/usr/local/polardb_backup_agent_current/bin/pgpipeline.conf
/usr/local/polardb_backup_agent_current/bin/pipeline.conf
/usr/local/polardb_backup_agent_current/bin/s3.conf
/usr/local/polardb_backup_agent_current/bin/wal.conf
/usr/local/polardb_backup_agent_current/bin/backupctl-fs2fs.sh
/usr/local/polardb_backup_agent_current/bin/backupctl-multifs2fs.sh
/usr/local/polardb_backup_agent_current/bin/download-dbs2fs.sh
/usr/local/polardb_backup_agent_current/bin/full-restorectl-dbs2pfs.sh
/usr/local/polardb_backup_agent_current/bin/full-restorectl-fs2pfs.sh
/usr/local/polardb_backup_agent_current/bin/full-restorectl-s32pfs.sh
/usr/local/polardb_backup_agent_current/bin/incre-restorectl-dbs2pfs.sh
/usr/local/polardb_backup_agent_current/bin/incre-restorectl-fs2pfs.sh
/usr/local/polardb_backup_agent_current/bin/incre-restorectl-s32pfs.sh
/usr/local/polardb_backup_agent_current/bin/recovery2fs-pitr-bydb.sh
/usr/local/polardb_backup_agent_current/bin/recovery2fs-pitr-byfs.sh
/usr/local/polardb_backup_agent_current/bin/recovery-pitr.sh
/usr/local/polardb_backup_agent_current/bin/regular-fullbackup-fs2fs.sh
/usr/local/polardb_backup_agent_current/bin/regular-fullbackup-pfs2dbs.sh
/usr/local/polardb_backup_agent_current/bin/regular-fullbackup-pfs2fs.sh
/usr/local/polardb_backup_agent_current/bin/regular-fullbackup-pfs2s3.sh
/usr/local/polardb_backup_agent_current/bin/regular-increbackup-fs2fs.sh
/usr/local/polardb_backup_agent_current/bin/regular-increbackup-pfs2dbs.sh
/usr/local/polardb_backup_agent_current/bin/regular-increbackup-pfs2fs.sh
/usr/local/polardb_backup_agent_current/bin/regular-increbackup-pfs2s3.sh
/usr/local/polardb_backup_agent_current/bin/restorectl-fs2fs.sh
/usr/local/polardb_backup_agent_current/bin/restore-initdb.sh
/usr/local/polardb_backup_agent_current/bin/backupctl-multifs2fs-cm.sh
/usr/local/polardb_backup_agent_current/bin/backupctl-multifs2http-cm.sh
/usr/local/polardb_backup_agent_current/bin/recovery2fs-pitr-bydb-cm.sh
/usr/local/polardb_backup_agent_current/bin/recovery2fs-pitr-byfs-cm.sh
/usr/local/polardb_backup_agent_current/bin/recovery2fs-pitr-byhttp-cm.sh
/usr/local/polardb_backup_agent_current/bin/regular-increbackup-fs2fs-cm.sh
/usr/local/polardb_backup_agent_current/bin/regular-increbackup-fs2http-cm.sh
/usr/local/polardb_backup_agent_current/bin/stop-increbackup-cm.sh
/usr/local/polardb_backup_agent_current/bin/install-httpfiled.sh
/usr/local/polardb_backup_agent_current/bin/regular-fullbackup-fs2dbs.sh
/usr/local/polardb_backup_agent_current/bin/regular-increbackup-fs2dbs.sh
/usr/local/polardb_backup_agent_current/bin/recovery-dbs2fs.sh
/usr/local/polardb_backup_agent_current/bin/regular-fullbackup-flex.sh
/usr/local/polardb_backup_agent_current/bin/regular-increbackup-flex.sh
/usr/local/polardb_backup_agent_current/bin/stop-increbackup-flex.sh
/usr/local/polardb_backup_agent_current/bin/regular-deltabackup-flex.sh
/usr/local/polardb_backup_agent_current/bin/recovery2fs-flex.sh
/usr/local/polardb_backup_agent_current/bin/backupgc
/usr/local/polardb_backup_agent_current/bin/backupgc.conf
/usr/local/polardb_backup_agent_current/bin/install-backupgcd.sh
/usr/local/polardb_backup_agent_current/bin/pbkcli
/usr/local/polardb_backup_agent_current/bin/sample.backup.conf
/usr/local/polardb_backup_agent_current/bin/VERSION
/usr/local/polardb_backup_agent_current/bin/CHANGELOG.md
/usr/local/polardb_backup_agent_current/log
/usr/local/polardb_backup_agent_current/script/dbstack/*

%post
# Compatible with the previous version
for file in /usr/local/polardb_backup_agent_current/bin/*
do
    sed -i "s/\"ManagerAddr\": \"127.0.0.1:1888\"/\"ManagerAddr\": \[\"127.0.0.1:1888\"\]/g" $file
done

echo "[Unit]" > %{SERVICE_FILE}
echo "Description=backup server daemon" >> %{SERVICE_FILE}
echo "Documentation=no" >> %{SERVICE_FILE}
echo "After=no" >> %{SERVICE_FILE}
echo "Wants=no" >> %{SERVICE_FILE}
echo "" >> %{SERVICE_FILE}
echo "[Service]" >> %{SERVICE_FILE}
echo "EnvironmentFile=no" >> %{SERVICE_FILE}
echo "ExecStart=/usr/local/polardb_backup_agent_current/bin/backup_ctl -config /usr/local/polardb_backup_agent_current/bin" >> %{SERVICE_FILE}
echo "ExecReload=/bin/kill -HUP $MAINPID" >> %{SERVICE_FILE}
echo "KillMode=process" >> %{SERVICE_FILE}
echo "Restart=on-failure" >> %{SERVICE_FILE}
echo "RestartSec=1s" >> %{SERVICE_FILE}
echo "" >> %{SERVICE_FILE}
echo "[Install]" >> %{SERVICE_FILE}
echo "WantedBy=multi-user.target" >> %{SERVICE_FILE}

systemctl enable %{SERVICE_NAME}

# Compatible with the previous version
sleep 1 && rpm -e PolarDB-O-backup-tool 1>/dev/null 2>/dev/null &

%preun
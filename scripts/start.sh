#!/bin/bash
touch backup_ctl.log
chmod 777 backup_ctl.log
cur_path=$(pwd)
cd /usr/local/polardb_o_backup_tool_current/bin
nohup ./backup_ctl > backup_ctl.log 2>&1 &
cd $cur_path
echo 'backup_ctl start!'

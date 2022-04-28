#!/bin/bash

# User should config the following params
mount=""    # eg: mount="/disk1/backup"
log=""      # eg: log="/disk1/log"
addr=""     # eg: addr=":8080"

# Create the systemd service and start. DO NOT EDIT.
SERVICE_FILE="/usr/lib/systemd/system/httpfiled.service"
SERVICE_NAME="httpfiled.service"
echo "[Unit]" > $SERVICE_FILE
echo "Description=backup server daemon" >> $SERVICE_FILE
echo "Documentation=no" >> $SERVICE_FILE
echo "After=no" >> $SERVICE_FILE
echo "Wants=no" >> $SERVICE_FILE
echo "" >> $SERVICE_FILE
echo "[Service]" >> $SERVICE_FILE
echo "EnvironmentFile=no" >> $SERVICE_FILE
echo "ExecStart=/usr/local/polardb_o_backup_tool_current/bin/http_file_server -mount $mount -addr $addr -log $log" >> $SERVICE_FILE
echo "ExecReload=/bin/kill -HUP $MAINPID" >> $SERVICE_FILE
echo "KillMode=process" >> $SERVICE_FILE
echo "Restart=on-failure" >> $SERVICE_FILE
echo "RestartSec=1s" >> $SERVICE_FILE
echo "" >> $SERVICE_FILE
echo "[Install]" >> $SERVICE_FILE
echo "WantedBy=multi-user.target" >> $SERVICE_FILE

systemctl enable $SERVICE_NAME
systemctl start $SERVICE_NAME


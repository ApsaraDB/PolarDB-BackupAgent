#!/bin/sh
if [ -z $PFSDIR ];then
  echo "[ERROR] \$PFSDIR not set yet"
  exit 1
fi

if [ -z $PGBACKUPTOOLHOME ];then
  echo "[ERROR] \$PGBACKUPTOOLHOME not set yet"
  exit 1
fi

sudo /usr/local/bin/pfs -C disk read $PFSDIR/global/pg_control > $PGBACKUPTOOLHOME/pg_control
echo "[INFO] execute backup-initdb.sh done!"
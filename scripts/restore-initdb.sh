if [ -z $PFSDIR ];then
  echo "[ERROR] \$PFSDIR not set yet"
  exit $1
fi

if [ -z $PGDATA ];then
  echo "[ERROR] \$PGDATA not set yet"
  exit $1
fi

sudo /usr/local/bin/pfs -C disk ls $PFSDIR/base | grep 'Dir' | awk -F' ' '{print $NF}' | while read line 
do 
  if [ ! -d "$PGDATA/base/$line" ]; then
    mkdir $PGDATA/base/$line
  fi
done
echo "[INFO] restore base dir done!"

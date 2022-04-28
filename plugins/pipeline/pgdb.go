package main

import (
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

type DBInfo struct {
	endpoint        string
	port            string
	username        string
	password        string
	database        string
	db              *sql.DB
	ftx             *sql.Tx
	btx             *sql.Tx
	role            int
	extensions      []string
	applicationName string
	version         int64
	releaseDate     int64
	execPGStopBK    bool
	dbType          string
	dbNodes         []string
	isClose         bool
}

type CommonDBConf struct {
	Endpoint        string `json:"Endpoint"`
	Port            string `json:"Port"`
	Username        string `json:"Username"`
	Password        string `json:"Password"`
	Database        string `json:"Database"`
	ApplicationName string `json:"ApplicationName"`
	DBType          string `json:"DBType"`
}

type BackupAccount struct {
	Endpoint string `json:"Endpoint"`
	Port     string `json:"Port"`
	User     string `json:"User"`
	Password string `json:"Password"`
	Database string `json:"Database"`
}

type MetaAccount struct {
	Endpoint string `json:"Endpoint"`
	Port     string `json:"Port"`
	User     string `json:"User"`
	Password string `json:"Password"`
	Database string `json:"Database"`
	DBType   string `json:"DBType"`
}

func initPgsqlDBInfo(dbConf *CommonDBConf) (*DBInfo, error) {
	var dbInfo DBInfo
	var err error

	dbInfo.endpoint = dbConf.Endpoint
	dbInfo.port = dbConf.Port
	dbInfo.username = dbConf.Username
	dbInfo.database = dbConf.Database
	dbInfo.applicationName = dbConf.ApplicationName

	dbInfo.dbType = "pgsql"

	if dbInfo.database == "" {
		dbInfo.database = "postgres"
	}

	if dbInfo.applicationName == "" {
		dbInfo.applicationName = "default"
	}

	decoded, err := base64.StdEncoding.DecodeString(dbConf.Password)
	if err != nil {
		return nil, errors.New(err.Error() + " input password:" + dbConf.Password)
	}
	dbInfo.password = string(decoded)

	err = openPsqlDB(&dbInfo)
	if err != nil {
		return nil, err
	}

	return &dbInfo, nil
}

func openPsqlDB(dbInfo *DBInfo) error {
	var err error

	dbUrl := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s fallback_application_name=%s sslmode=disable",
		dbInfo.endpoint, dbInfo.username, dbInfo.password, dbInfo.database, dbInfo.port, dbInfo.applicationName)

	tryMax := 3
	for i := 0; i < tryMax; i++ {
		if dbInfo.db, err = sql.Open("postgres", dbUrl); err != nil {
			time.Sleep(5 * time.Second)
		} else {
			break
		}
	}

	if err != nil {
		err = errors.New("try 3 times, connect to psql db failed:" + err.Error())
		return err
	}

	for i := 0; i < tryMax; i++ {
		if err = dbInfo.db.Ping(); err != nil {
			time.Sleep(5 * time.Second)
		} else {
			break
		}
	}

	if err != nil {
		err = errors.New("try 3 times, ping psql db failed:" + err.Error())
		return err
	}

	return nil
}

func initMysqlDBInfo(dbConf *CommonDBConf) (*DBInfo, error) {
	var dbInfo DBInfo
	var err error

	dbInfo.endpoint = dbConf.Endpoint
	dbInfo.port = dbConf.Port
	dbInfo.username = dbConf.Username
	dbInfo.database = dbConf.Database
	dbInfo.applicationName = dbConf.ApplicationName

	dbInfo.dbType = "mysql"

	if dbInfo.database == "" {
		dbInfo.database = "postgres"
	}

	if dbInfo.applicationName == "" {
		dbInfo.applicationName = "default"
	}

	decoded, err := base64.StdEncoding.DecodeString(dbConf.Password)
	if err != nil {
		return nil, err
	}
	dbInfo.password = string(decoded)

	if len(dbInfo.password) > 1 && dbInfo.password[len(dbInfo.password)-1] == '\n' {
		dbInfo.password = dbInfo.password[:len(dbInfo.password)-1]
	}

	dbUrl := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbInfo.username, dbInfo.password, dbInfo.endpoint, dbInfo.port, dbInfo.database)

	tryMax := 3
	for i := 0; i < tryMax; i++ {
		if dbInfo.db, err = sql.Open("mysql", dbUrl); err != nil {
			time.Sleep(5 * time.Second)
		} else {
			break
		}
	}

	if err != nil {
		err = errors.New("try 3 times, connect to mysql db failed:" + err.Error())
		return &dbInfo, err
	}

	for i := 0; i < tryMax; i++ {
		if err = dbInfo.db.Ping(); err != nil {
			time.Sleep(5 * time.Second)
		} else {
			break
		}
	}

	if err != nil {
		err = errors.New("try 3 times, ping mysql db failed:" + err.Error())
		return nil, err
	}

	return &dbInfo, nil
}

func initFullBackupFlag(dbInfo *DBInfo) error {
	if dbInfo == nil {
		return errors.New("db is nil when try to init full backup flag")
	}

	if dbInfo.dbType != "pgsql" {
		return errors.New("Not support db type")
	}

	sql := "CREATE TABLE IF NOT EXISTS polarfullbakstatus ();"
	rows, err := queryDB(dbInfo, sql)
	if err == nil {
		defer rows.Close()
	}
	return err
}

func setFullBackupFlag(dbInfo *DBInfo) error {
	tx, err := dbInfo.db.Begin()
	if err != nil {
		return err
	}
	dbInfo.ftx = tx
	_, err = dbInfo.ftx.Exec("BEGIN; LOCK TABLE polarfullbakstatus IN ACCESS EXCLUSIVE MODE;")
	if err != nil {
		return err
	}
	return nil
}

func setFullBackupDone(dbInfo *DBInfo) error {
	return dbInfo.ftx.Commit()
}

func waitFullBackupDone(dbInfo *DBInfo) error {
	rows, err := dbInfo.db.Query("select * from polarfullbakstatus;")
	if rows != nil {
		defer rows.Close()
	}
	return err
}

func initBlockBackupFlag(dbInfo *DBInfo) error {
	if dbInfo == nil {
		return errors.New("db is nil when try to init full backup flag")
	}

	if dbInfo.dbType != "pgsql" {
		return errors.New("Not support db type")
	}

	sql := "CREATE TABLE IF NOT EXISTS polardeltabakstatus ();"
	rows, err := queryDB(dbInfo, sql)
	if err == nil {
		defer rows.Close()
	}
	return err
}

func setBlockBackupFlag(dbInfo *DBInfo) error {
	tx, err := dbInfo.db.Begin()
	if err != nil {
		return err
	}
	dbInfo.btx = tx
	_, err = dbInfo.btx.Exec("BEGIN; LOCK TABLE polardeltabakstatus IN ACCESS EXCLUSIVE MODE;")
	if err != nil {
		return err
	}
	return nil
}

func setBlockBackupDone(dbInfo *DBInfo) error {
	return dbInfo.btx.Commit()
}

func waitBlockBackupDone(dbInfo *DBInfo) error {
	rows, err := dbInfo.db.Query("select * from polardeltabakstatus;")
	if rows != nil {
		defer rows.Close()
	}
	return err
}

func queryDB(dbInfo *DBInfo, sql string) (*sql.Rows, error) {
	rows, err := dbInfo.db.Query("/* rds internal mark */ " + sql)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func pgStartBackup(dbinfo *DBInfo) error {
	exclusiveErr := "pq: a backup is already in progress"

	rows, err := queryDB(dbinfo, "select pg_start_backup('polardb_backup_agent', false, true);")
	if err == nil {
		defer rows.Close()
	}

	if err != nil && err.Error() == exclusiveErr {
		err = pgStopBackup(dbinfo)
		if err != nil {
			return errors.New(exclusiveErr + ", try stop backup and new error occur: " + err.Error())
		}

		rows, err = queryDB(dbinfo, "select pg_start_backup('polardb_backup_agent', false, true);")
		if err == nil {
			defer rows.Close()
		} else {
			return errors.New("try start backup again and error occur: " + err.Error())
		}
	}

	return err
}

func pgStopBackup(dbinfo *DBInfo) error {
	rows, err := queryDB(dbinfo, "select pg_stop_backup(true, false);")
	if err == nil {
		defer rows.Close()
	}
	return err
}

func checkMaster(dbinfo *DBInfo) (bool, error) {
	rows, err := queryDB(dbinfo, "select pg_is_in_recovery()")
	if err != nil {
		return false, err
	}
	defer rows.Close()

	var isInRecovery bool

	if rows.Next() {
		if err := rows.Scan(&isInRecovery); err != nil {
			return false, err
		}

		ismaster := !isInRecovery

		return ismaster, nil
	}

	return false, errors.New("empty when select pg_is_in_recovery")
}
func pgSwitchWal(dbinfo *DBInfo) error {
	rows, err := queryDB(dbinfo, "select pg_switch_wal();")
	if err == nil {
		defer rows.Close()
	}
	return err
}

func pgCountWal(dbinfo *DBInfo, instanceid string, walname string) (bool, error) {
	sql := `select * from backup_meta where instanceid=$1 and file=$2`
	rows, err := dbinfo.db.Query(sql, instanceid, walname)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			return true, nil
		}
	}
	return false, err
}

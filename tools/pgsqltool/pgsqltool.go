package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
)

type DBInfo struct {
	endpoint        string
	port            string
	username        string
	password        string
	database        string
	db              *sql.DB
	role            int
	extensions      []string
	applicationName string
	version         int64
	releaseDate     int64
	execPGStopBK    bool
}

type PgDBConf struct {
	Endpoint        string `json:"Endpoint"`
	Port            string `json:"Port"`
	Username        string `json:"Username"`
	Password        string `json:"Password"`
	Database        string `json:"Database"`
	ApplicationName string `json:"ApplicationName"`
}

func initDBInfo(pgdbConf PgDBConf) (*DBInfo, error) {
	var dbInfo DBInfo
	var err error

	dbInfo.endpoint = pgdbConf.Endpoint
	dbInfo.port = pgdbConf.Port
	dbInfo.username = pgdbConf.Username
	dbInfo.password = pgdbConf.Password
	dbInfo.database = pgdbConf.Database
	dbInfo.applicationName = pgdbConf.ApplicationName

	dbUrl := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s fallback_application_name=%s sslmode=disable",
		dbInfo.endpoint, dbInfo.username, dbInfo.password, dbInfo.database, dbInfo.port, dbInfo.applicationName)

	if dbInfo.db, err = sql.Open("postgres", dbUrl); err != nil {
		err = errors.New("connect to db failed:" + err.Error())
		return &dbInfo, err
	}

	if err = dbInfo.db.Ping(); err != nil {
		err = errors.New("ping db failed" + err.Error())
		return &dbInfo, err
	}

	return &dbInfo, nil
}

func queryDB(dbInfo *DBInfo, sql string) (*sql.Rows, error) {
	rows, err := dbInfo.db.Query("/* rds internal mark */ " + sql)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func pgStartBackup(dbinfo *DBInfo) error {
	rows, err := queryDB(dbinfo, "select pg_start_backup(true, false, true);")
	if err == nil {
		defer rows.Close()
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

func pgSwitchWal(dbinfo *DBInfo) error {
	rows, err := queryDB(dbinfo, "select pg_switch_wal();")
	if err == nil {
		defer rows.Close()
	}
	return err
}

func checkIfMaster(dbinfo *DBInfo) (bool, error) {
	rows, err := queryDB(dbinfo, "select pg_is_in_recovery();")
	if err == nil {
		defer rows.Close()
	}
	if err != nil {
		return false, err
	}
	var result string
	for rows.Next() {
		err = rows.Scan(&result)
		if err != nil {
			return false, err
		}
		if result == "true" {
			return false, nil
		} else if result == "false" {
			return true, nil
		} else {
			return false, errors.New("invalid query result from select pg_is_in_recovery()")
		}
	}
	return false, errors.New("invalid query result from select pg_is_in_recovery()")
}

var host = flag.String("host", "", "pg server ip")
var port = flag.String("port", "", "pg server port")
var user = flag.String("user", "", "super user name")
var pwd = flag.String("pwd", "", "password of super user")
var db = flag.String("db", "", "one database name")
var cmd = flag.String("cmd", "", "comand eg: repairbackup")

func main() {
	flag.Parse()

	isvalid := true
	if *host == "" {
		fmt.Println("[Error] param of host is null")
		isvalid = false
	}
	if *port == "" {
		fmt.Println("[Error] param of port is null")
		isvalid = false
	}
	if *user == "" {
		fmt.Println("[Error] param of user is null")
		isvalid = false
	}
	if *pwd == "" {
		fmt.Println("[Error] param of pwd is null")
		isvalid = false
	}
	if *db == "" {
		fmt.Println("[Error] param of db is null")
		isvalid = false
	}
	if *cmd == "" {
		fmt.Println("[Error] param of cmd is null")
		isvalid = false
	}

	if !isvalid {
		return
	}

	var pgdbconf PgDBConf
	pgdbconf.Endpoint = *host
	pgdbconf.Port = *port
	pgdbconf.Username = *user
	pgdbconf.Password = *pwd
	pgdbconf.Database = *db
	pgdbconf.ApplicationName = "none"

	dbinfo, err := initDBInfo(pgdbconf)
	if err != nil {
		fmt.Println("[Error] init db faild ", err.Error())
		return
	}
	if *cmd == "repairbackup" {
		err = pgStopBackup(dbinfo)
		if err != nil {
			fmt.Println("[ERROR] Execute ", *cmd, " failed: ", err.Error())
		} else {
			fmt.Println("[INFO] Execute ", *cmd, " successfully")
		}
	} else if *cmd == "ismaster" {
		ret, err := checkIfMaster(dbinfo)
		if err != nil {
			fmt.Println("[ERROR] Execute ", *cmd, " failed: ", err.Error())
		} else {
			if ret {
				fmt.Println("TRUE")
			} else {
				fmt.Println("FALSE")
			}
		}
	} else if *cmd == "switchwal" {
		err = pgSwitchWal(dbinfo)
		if err != nil {
			fmt.Println("[ERROR] Execute ", *cmd, " failed: ", err.Error())
		} else {
			fmt.Println("[INFO] Execute ", *cmd, " successfully")
		}
	} else {
		fmt.Println("[Error] not support cmd:", *cmd)
	}

}

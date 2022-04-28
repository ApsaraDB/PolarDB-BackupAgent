package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"path"
	"strconv"

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
	rows, err := dbInfo.db.Query("/* polar-o metatool internal mark */ " + sql)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func exportMeta(dbinfo *DBInfo, mtype string, dst string) error {
	if mtype != "wal" {
		return errors.New("not support meta type: " + mtype)
	}

	rows, err := queryDB(dbinfo, "select file,starttime,endtime from backup_meta where backuptype = 'wal';")
	if err != nil {
		return err
	}
	defer rows.Close()

	type Record struct {
		File      string
		Starttime int64
		Endtime   int64
	}

	var records []Record
	for rows.Next() {
		var file string
		var starttime int64
		var endtime int64

		err = rows.Scan(&file, &starttime, &endtime)
		if err != nil {
			return err
		}

		records = append(records, Record{
			File:      file,
			Starttime: starttime,
			Endtime:   endtime,
		})
	}

	n := len(records)
	if n == 0 {
		return errors.New("no such records in backup_meta")
	}

	mfile, err := os.Create(path.Join(dst, "wals.pg_o_backup_meta"))
	if err != nil {
		return err
	}
	defer mfile.Close()

	_, err = mfile.Write([]byte(strconv.Itoa(n) + "\n"))
	if err != nil {
		return err
	}

	for i := 0; i < n; i++ {
		record := records[i].File + "," + strconv.FormatUint(uint64(records[i].Starttime), 10) + "," + strconv.FormatUint(uint64(records[i].Endtime), 10) + "\n"
		buf := []byte(record)
		_, err := mfile.Write(buf)
		if err != nil {
			return err
		}
	}

	fmt.Printf("[INFO] export %d records to %s done.\n", n, path.Join(dst, "wals.pg_o_backup_meta"))
	return nil
}

var host = flag.String("host", "", "pg server ip")
var port = flag.String("port", "", "pg server port")
var user = flag.String("user", "", "super user name")
var pwd = flag.String("pwd", "", "password of super user")
var db = flag.String("db", "", "one database name")
var cmd = flag.String("cmd", "", "comand eg: export")
var mtype = flag.String("type", "", "comand eg: full, wal")
var dst = flag.String("dst", "", "comand eg: /tmp")

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
	if *mtype == "" {
		fmt.Println("[Error] param of type is null")
		isvalid = false
	}
	if *dst == "" {
		fmt.Println("[Error] param of dst is null")
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
		fmt.Printf("[Error] init db failed: %s\n", err.Error())
		return
	}
	if *cmd == "export" {
		err := exportMeta(dbinfo, *mtype, *dst)
		if err != nil {
			fmt.Printf("[Error] export meta failed: %s\n", err.Error())
		}
	} else {
		fmt.Printf("[Error] not support cmd: %s\n", *cmd)
	}

}

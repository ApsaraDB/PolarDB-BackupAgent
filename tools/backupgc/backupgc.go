package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"gopkg.in/natefinch/lumberjack.v2"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

type BackupAccount struct {
	Endpoint string `json:"Endpoint"`
	Port     string `json:"Port"`
	User     string `json:"User"`
	Password string `json:"Password"`
	Database string `json:"Database"`
}

type BackupStorageSpace struct {
	StorageType string    `json:"StorageType"`
	NameSpace   string    `json:"NameSpace"`
	Locations   Locations `json:"Locations"`
}

type Locations struct {
	S3    S3    `json:"S3"`
	FTP   FTP   `json:"FTP"`
	Local Local `json:"Local"`
	DBS   DBS   `json:"DBS"`
	HTTP  HTTP  `json:"HTTP"`
}

type HTTP struct {
	Endpoint string `json:"Endpoint"`
}

type DBS struct {
	Endpoint string `json:"Endpoint"`
}

type S3 struct {
	Endpoint  string `json:"Endpoint"`
	Bucket    string `json:"Bucket"`
	Accesskey string `json:"Accesskey"`
	Secretkey string `json:"Secretkey"`
	Region    string `json:"Region"`
	Secure    bool   `json:"Secure"`
}

type FTP struct {
	Address   string `json:"Address"`
	Port      string `json:"Port"`
	Path      string `json:"Path"`
	Anonymous bool   `json:"Anonymous"`
	Username  string `json:"Username"`
	Password  string `json:"Password"`
}

type Local struct {
	Path string `json:"Path"`
}

type LogConf struct {
	Logdir      string `json:"Logdir"`
	Compress    bool   `json:"Compress"`
	RotateSize  int    `json:"RotateSize"`
	MaxCounts   int    `json:"MaxCounts"`
	MaxDays     int    `json:"MaxDays"`
	ForceStdout bool   `json:"ForceStdout"`
}

type GCConf struct {
	Mode               string             `json:"Mode"`
	TimeReserve        string             `json:"TimeReserve"`
	FullsReserve       int                `json:"FullsReserve"`
	WalsReserve        int                `json:"WalsReserve"`
	GCPeriod           int                `json:"GCPeriod"`
	BackupAccount      BackupAccount      `json:"BackupAccount"`
	BackupStorageSpace BackupStorageSpace `json:"BackupStorageSpace"`
	Log                LogConf            `json:"Log"`
}

type Full struct {
	name      string
	starttime int
	endtime   int
}

type Wal struct {
	name      string
	starttime int
	endtime   int
}

type FullsMeta struct {
	Size  int
	Fulls []Full
}

type WalsMeta struct {
	Size int
	Wals []Wal
}

type fileSystem struct {
	funcs  map[string]func(interface{}, map[string]interface{}) (interface{}, error)
	handle interface{}
	name   string
}

type GCSever struct {
	conf    GCConf
	logger  *log.Logger
	backend fileSystem
}

const(
	DefaultConfigPath = "/usr/local/polardb_o_backup_tool_current/bin"
)

func (gcs *GCSever) CapturePanic() {
	defer func() {
		if r := recover(); r != nil {
			errStr := string(debug.Stack())
			fmt.Printf("backupgc panic from:\n%s", errStr)
			gcs.logger.Printf("backupgc panic from:\n%s", errStr)
		}
	}()
}

var (
	configPath string
)

func init() {
	flag.StringVar(&configPath, "config", "", "the dir include backupgc.conf")
	flag.Parse()
}

func main() {
	var gcs GCSever

	err := gcs.Init()
	if err != nil {
		fmt.Println("[ERROR] gc server init failed: %s", err.Error())
		return
	}

	// do backup gc
	err = gcs.Gc()
	if err != nil {
		gcs.logger.Printf("[ERROR] gc failed : %s", err.Error())
	}
}

func (gcs *GCSever) Init() error {
	err := gcs.initConf()
	if err != nil {
		fmt.Println("[ERROR] init conf failed: %s", err.Error())
		return err
	}

	err = gcs.InitLog()
	if err != nil {
		return err
	}

	err = gcs.InitBackend()
	if err != nil {
		gcs.logger.Printf("[ERROR] init backend failed : %s", err.Error())
		return err
	}

	return nil
}

func (gcs *GCSever) Gc() error {
	for {
		err := gcs.GcCore()
		if err != nil {
			gcs.logger.Printf("[ERROR] execute gc failed : %s", err.Error())
		}
		time.Sleep(time.Duration(gcs.conf.GCPeriod) * time.Second)
	}

	return nil
}

func (gcs *GCSever) GcCore() error {
	initParam := make(map[string]interface{})

	initParam["path"] = ""
	_instances, err := gcs.backend.funcs["readdir"](gcs.backend.handle, initParam)
	if err != nil {
		gcs.logger.Printf("[ERROR] gc get instances failed: %s", err.Error())
		return err
	}
	gcs.logger.Printf("[INFO] intances: %s\n", _instances)

	failedCount := 0

	instances := _instances.([]string)
	for _, instance := range instances {
		gcs.logger.Printf("[INFO] ################## gc full backup of intance: %s ##################\n", instance)

		err = gcs.GcFullBackups(instance)
		if err != nil {
			gcs.logger.Printf("[ERROR] execute gc fullbackups for instance %s failed : %s", instance, err.Error())
			failedCount = failedCount + 1
		}

		gcs.logger.Printf("[INFO] ################## gc wals of intance: %s ##################\n", instance)

		err := gcs.GcWals(instance)
		if err != nil {
			gcs.logger.Printf("[ERROR] execute gc wals for instance %s failed : %s", instance, err.Error())
			failedCount = failedCount + 1
		}
	}

	if failedCount > 0 {
		err := errors.New("execute gc failed " + strconv.Itoa(failedCount) + " times")
		return err
	}

	return nil
}

func (gcs *GCSever) GetFullsMeta(instance string) (FullsMeta, error) {
	var fullsMeta FullsMeta

	param := make(map[string]interface{})
	param["name"] = instance + "/fullmetas/fullbackups.pg_o_backup_meta"
	metaFile, err := gcs.backend.funcs["open"](gcs.backend.handle, param)
	if err != nil {
		gcs.logger.Printf("[ERROR] open meta file failed. err[%s]\n", err.Error())
		return fullsMeta, err
	}

	metaReader, ok := metaFile.(io.Reader)
	if !ok {
		err = errors.New("invalid metaFile")
		return fullsMeta, err
	}

	var wbuf []byte
	buf := make([]byte, 1024*1024)
	for {
		nr, er := metaReader.Read(buf)
		if nr > 0 {
			wbuf = append(wbuf, buf[0:nr]...)
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}

	param["file"] = metaFile
	gcs.backend.funcs["close"](gcs.backend.handle, param)

	i := 0
	for ; i < len(wbuf); i++ {
		if wbuf[i] == '\n' {
			break
		}
	}
	n, err := strconv.Atoi(string(wbuf[0:i]))
	if err != nil {
		gcs.logger.Printf("[ERROR] convert first line of fullbackups.pg_o_backup_meta failed. err[%s]\n", err.Error())
		return fullsMeta, err
	}

	fullsMeta.Size = n

	var b int
	i = i + 1
	b = i
	l := len(wbuf)
	for ; i < l; i++ {
		if wbuf[i] == '\n' {
			record := string(wbuf[b:i])

			sp := strings.Split(record, ",")
			/*
			 * backward compatibility: add 4th column that represent type for block backup, no 4th column in previous version
			 */
			 if len(sp) < 3 || len(sp) > 4 {
				gcs.logger.Printf("[ERROR] invalid record: %s in fullbackups.pg_o_backup_meta\n", record)
				return fullsMeta, errors.New("invalid record: " + record + "in fullbackups.pg_o_backup_meta")
			}

			var full Full
			full.name = sp[0]
			starttime, err := strconv.Atoi(sp[1])
			if err != nil {
				gcs.logger.Printf("[ERROR] convert record of fullbackups.pg_o_backup_meta failed, err[%s]\n", err.Error())
				return fullsMeta, err
			}
			full.starttime = starttime

			endtime, err := strconv.Atoi(sp[2])
			if err != nil {
				gcs.logger.Printf("[ERROR] convert record of fullbackups.pg_o_backup_meta failed, err[%s]\n", err.Error())
				return fullsMeta, err
			}
			full.endtime = endtime

			fullsMeta.Fulls = append(fullsMeta.Fulls, full)

			b = i + 1
		}
	}

	if fullsMeta.Size != len(fullsMeta.Fulls) {
		gcs.logger.Printf("[ERROR] invalid fullbackups.pg_o_backup_meta. n is %d, but len of records is %d\n", fullsMeta.Size, len(fullsMeta.Fulls))
		return fullsMeta, errors.New("invalid fullbackups.pg_o_backup_meta")
	}

	return fullsMeta, nil
}

func (gcs *GCSever) GetWalsMeta(instance string) (WalsMeta, error) {
	var walsMeta WalsMeta

	param := make(map[string]interface{})
	param["name"] = instance + "/increbk/wals.pg_o_backup_meta"
	metaFile, err := gcs.backend.funcs["open"](gcs.backend.handle, param)
	if err != nil {
		gcs.logger.Printf("[ERROR] open meta file failed. err[%s]\n", err.Error())
		return walsMeta, err
	}

	metaReader, ok := metaFile.(io.Reader)
	if !ok {
		err = errors.New("invalid metaFile")
		return walsMeta, err
	}

	var wbuf []byte
	buf := make([]byte, 1024*1024)
	for {
		nr, er := metaReader.Read(buf)
		if nr > 0 {
			wbuf = append(wbuf, buf[0:nr]...)
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}

	param["file"] = metaFile
	gcs.backend.funcs["close"](gcs.backend.handle, param)

	i := 0
	for ; i < len(wbuf); i++ {
		if wbuf[i] == '\n' {
			break
		}
	}
	n, err := strconv.Atoi(string(wbuf[0:i]))
	if err != nil {
		gcs.logger.Printf("[ERROR] convert first line of wals.pg_o_backup_meta failed. err[%s]\n", err.Error())
		return walsMeta, err
	}

	walsMeta.Size = n

	var b int
	i = i + 1
	b = i
	l := len(wbuf)
	for ; i < l; i++ {
		if wbuf[i] == '\n' {
			record := string(wbuf[b:i])

			sp := strings.Split(record, ",")
			if len(sp) != 3 {
				gcs.logger.Printf("[ERROR] invalid record: %s in wals.pg_o_backup_meta\n", record)
				return walsMeta, errors.New("invalid record: " + record + "in wals.pg_o_backup_meta")
			}

			var wal Wal
			wal.name = sp[0]
			starttime, err := strconv.Atoi(sp[1])
			if err != nil {
				gcs.logger.Printf("[ERROR] convert record of wals.pg_o_backup_meta failed, err[%s]\n", err.Error())
				return walsMeta, err
			}
			wal.starttime = starttime

			endtime, err := strconv.Atoi(sp[2])
			if err != nil {
				gcs.logger.Printf("[ERROR] convert record of wals.pg_o_backup_meta failed, err[%s]\n", err.Error())
				return walsMeta, err
			}
			wal.endtime = endtime

			walsMeta.Wals = append(walsMeta.Wals, wal)

			b = i + 1
		}
	}

	if walsMeta.Size != len(walsMeta.Wals) {
		gcs.logger.Printf("[ERROR] invalid wals.pg_o_backup_meta. n is %d, but len of records is %d\n", walsMeta.Size, len(walsMeta.Wals))
		return walsMeta, errors.New("invalid wals.pg_o_backup_meta")
	}

	return walsMeta, nil
}

func (gcs *GCSever) UpdateFullsMeta(instance string, fullsMeta *FullsMeta, del int) error {
	param := make(map[string]interface{})
	filename := instance + "/fullmetas/fullbackups.pg_o_backup_meta"
	param["name"] = filename
	_, err := gcs.backend.funcs["unlink"](gcs.backend.handle, param)
	if err != nil {
		gcs.logger.Printf("[ERROR] unlink[%s] failed, err[%s]\n", param["name"], err.Error())
		return err
	}

	n := fullsMeta.Size - del

	backendFile, err := gcs.backend.funcs["create"](gcs.backend.handle, param)
	if err != nil {
		gcs.logger.Printf("[ERROR] recreate full meta failed, err[%s]\n", err.Error())
		return err
	}

	param["file"] = filename
	defer gcs.backend.funcs["close"](gcs.backend.handle, param)

	backendWriter, ok := backendFile.(io.Writer)
	if !ok {
		err = errors.New("invalid backendFile")
		return err
	}

	backendWriter.Write([]byte(strconv.Itoa(n) + "\n"))

	for i := del; i < fullsMeta.Size; i++ {
		record := fullsMeta.Fulls[i].name + "," + strconv.FormatUint(uint64(fullsMeta.Fulls[i].starttime), 10) + "," + strconv.FormatUint(uint64(fullsMeta.Fulls[i].endtime), 10) + "\n"
		buf := []byte(record)
		_, ew := backendWriter.Write(buf)
		if ew != nil {
			return ew
		}
	}

	return nil
}

func (gcs *GCSever) UpdateWalsMeta(instance string, walsMeta *WalsMeta, del int) error {
	param := make(map[string]interface{})
	filename := instance + "/increbk/wals.pg_o_backup_meta"
	param["name"] = filename
	_, err := gcs.backend.funcs["unlink"](gcs.backend.handle, param)
	if err != nil {
		gcs.logger.Printf("[ERROR] unlink[%s] failed, err[%s]\n", param["name"], err.Error())
		return err
	}

	n := walsMeta.Size - del

	backendFile, err := gcs.backend.funcs["create"](gcs.backend.handle, param)
	if err != nil {
		gcs.logger.Printf("[ERROR] recreate full meta failed, err[%s]\n", err.Error())
		return err
	}

	param["file"] = filename
	defer gcs.backend.funcs["close"](gcs.backend.handle, param)

	backendWriter, ok := backendFile.(io.Writer)
	if !ok {
		err = errors.New("invalid backendFile")
		return err
	}

	backendWriter.Write([]byte(strconv.Itoa(n) + "\n"))

	for i := del; i < walsMeta.Size; i++ {
		record := walsMeta.Wals[i].name + "," + strconv.FormatUint(uint64(walsMeta.Wals[i].starttime), 10) + "," + strconv.FormatUint(uint64(walsMeta.Wals[i].endtime), 10) + "\n"
		buf := []byte(record)
		_, ew := backendWriter.Write(buf)
		if ew != nil {
			return ew
		}
	}

	return nil
}

func (gcs *GCSever) getResverveTime() (int, error) {
	// supoort s/h/d
	tf := gcs.conf.TimeReserve
	if len(tf) < 2 {
		return 0, errors.New("invalid TimeReserve, please check the backupgc.conf")
	}

	t2s := 1
	ttype := tf[len(tf)- 1]
	switch ttype {
	case 's':
		t2s = 1
		break
	case 'h':
		t2s = 3600
		break
	case 'd':
		t2s = 3600 * 24
		break
	default:
		return 0, errors.New("invalid TimeReserve, only support s/h/d please check the backupgc.conf")
	}

	_tfnum := tf[:len(tf) - 1]
	tfnum, err := strconv.Atoi(_tfnum)
	if err != nil {
		return 0, errors.New("invalid TimeReserve, please check the backupgc.conf")
	}

	tfnum = tfnum * t2s
	if tfnum == 0 {
		return 0, errors.New("invalid TimeReserve, should not be zero, please check the backupgc.conf")
	}

	return tfnum, nil
}

func (gcs *GCSever) GcFullBackups(instance string) error {
	// lock and defer unlock
	// get meta
	fullsMeta, err := gcs.GetFullsMeta(instance)
	if err != nil {
		return err
	}

	param := make(map[string]interface{})
	if gcs.conf.Mode == "number" {
		// delete backup if need
		if fullsMeta.Size > gcs.conf.FullsReserve {
			del := fullsMeta.Size - gcs.conf.FullsReserve
			for i := 0; i < del; i++ {
				backupid := fullsMeta.Fulls[i].name
				param["path"] = instance + "/" + backupid
				_, err := gcs.backend.funcs["rmdir"](gcs.backend.handle, param)
				if err != nil {
					gcs.logger.Printf("[ERROR] delete backup failed : %s", err.Error())
					return err
				}
				gcs.logger.Printf("[INFO] delete full backup: %s done", param["path"])
			}

			// update meta
			// err = gcs.UpdateFullsMeta(instance, &fullsMeta, del)
			// if err != nil {
			// 	return err
			// }
		}
	} else if gcs.conf.Mode == "time" {
		param["path"] = "/" + instance
		_backupids, err := gcs.backend.funcs["readdir"](gcs.backend.handle, param)
		if err != nil {
			gcs.logger.Printf("[ERROR] gc get backupids failed: %s", err.Error())
			return err
		}
		backupids := _backupids.([]string)
		gcs.logger.Printf("[INFO] found %d full backups: %s", len(backupids), backupids)

		backupidsSet := make(map[string]struct{})
		for i := 0; i < len(backupids); i++ {
			backupidsSet[backupids[i]] = struct{}{}
		}

		reserveTime, err := gcs.getResverveTime()
		if err != nil {
			return err
		}

		failedCount := 0
		delCount := 0
		curTime := time.Now().Unix()
		for i := 0; i < fullsMeta.Size; i++ {
			backupid := fullsMeta.Fulls[i].name
			startTime := fullsMeta.Fulls[i].starttime

			if _, ok := backupidsSet[backupid]; ok {
				if int(curTime) - startTime > reserveTime {
					gcs.logger.Printf("[INFO] found full backup: %s of instance: %s backup at %d, exceed %s", backupid, instance, startTime, gcs.conf.TimeReserve)
					param["path"] = instance + "/" + backupid
					_, err := gcs.backend.funcs["rmdir"](gcs.backend.handle, param)
					if err != nil {
						gcs.logger.Printf("[ERROR] delete backup failed : %s", err.Error())
						failedCount = failedCount + 1
						continue
					}
					delCount = delCount + 1
					gcs.logger.Printf("[INFO] delete full backup: %s of instance: %s done", param["path"], instance)
				}
			}
		}

		gcs.logger.Printf("[INFO] delete %d full backups of instance: %s exceed %s", delCount, instance, gcs.conf.TimeReserve)

		if failedCount > 0 {
			gcs.logger.Printf("[ERROR] delete backup failed: %d times ", failedCount)
			return errors.New("failed occur when try delete full backup")
		}
	} else {
		return errors.New("not support gc mode, please check the backupgc.conf")
	}

	return nil
}

func (gcs *GCSever) GcWals(instance string) error {
	// lock and defer unlock
	// get meta
	walsMeta, err := gcs.GetWalsMeta(instance)
	if err != nil {
		return err
	}

	param := make(map[string]interface{})
	if gcs.conf.Mode == "number" {
		// delete backup if need
		param := make(map[string]interface{})
		if walsMeta.Size > gcs.conf.WalsReserve {
			del := walsMeta.Size - gcs.conf.WalsReserve
			for i := 0; i < del; i++ {
				walname := walsMeta.Wals[i].name
				param["name"] = instance + "/increbk/" + walname
				_, err := gcs.backend.funcs["unlink"](gcs.backend.handle, param)
				if err != nil {
					gcs.logger.Printf("[ERROR] delete wal failed : %s", err.Error())
					return err
				}
				gcs.logger.Printf("[INFO] delete wal: %s done", instance+"/increbk/"+walname)
			}

			// update meta
			err = gcs.UpdateWalsMeta(instance, &walsMeta, del)
			if err != nil {
				return err
			}
		}
	} else if gcs.conf.Mode == "time" {
		param["path"] = "/" + instance + "/increbk"
		_wals, err := gcs.backend.funcs["readdir"](gcs.backend.handle, param)
		if err != nil {
			gcs.logger.Printf("[ERROR] gc get wals failed: %s", err.Error())
			return err
		}
		wals := _wals.([]string)
		gcs.logger.Printf("[INFO] found %d wals in backup set of instance %s", len(wals), instance)

		walsSet := make(map[string]struct{})
		for i := 0; i < len(wals); i++ {
			walsSet[wals[i]] = struct{}{}
		}

		curTime := time.Now().Unix()

		reserveTime, err := gcs.getResverveTime()
		if err != nil {
			return err
		}

		// wals that nearst the earliest reserve time should reserve
		earliestReserveTime := int(curTime) - reserveTime
		maxStarttime := 0
		var nearstWals []string
		for i := 0; i < walsMeta.Size; i++ {
			wal := walsMeta.Wals[i].name
			startTime := walsMeta.Wals[i].starttime
			if startTime < earliestReserveTime{
				if startTime > maxStarttime {
					nearstWals = nil
					nearstWals = append(nearstWals, wal)
					maxStarttime = startTime
				} else if startTime == maxStarttime {
					nearstWals = append(nearstWals, wal)
				}
			}
		}

		failedCount := 0
		delCount := 0
		for i := 0; i < walsMeta.Size; i++ {
			wal := walsMeta.Wals[i].name
			startTime := walsMeta.Wals[i].starttime
			endTime := walsMeta.Wals[i].endtime

			// do not delete history file whose starttime and endtime are zero
			if startTime == 0 && endTime == 0 {
				continue
			}

			// nearst wals should reserve
			for _, nw := range(nearstWals) {
				if nw == wal {
					continue
				}
			}

			if _, ok := walsSet[wal]; ok {
				if int(curTime) - startTime > reserveTime {
					gcs.logger.Printf("[INFO] found wal: %s of instance: %s start at %d, exceed %s", wal, instance, endTime, gcs.conf.TimeReserve)
					param["name"] = instance + "/increbk/" + wal
					_, err := gcs.backend.funcs["unlink"](gcs.backend.handle, param)
					if err != nil {
						gcs.logger.Printf("[ERROR] delete wal failed : %s", err.Error())
						failedCount = failedCount + 1
						continue
					}
					delCount = delCount + 1
					gcs.logger.Printf("[INFO] delete wal: %s of instance: %s done", param["name"], instance)
				}
			}
		}

		gcs.logger.Printf("[INFO] delete %d wals of instance: %s exceed %s", delCount, instance, gcs.conf.TimeReserve)

		if failedCount > 0 {
			gcs.logger.Printf("[ERROR] delete wal of instance: %s failed: %d times ", instance, failedCount)
			return errors.New("failed occur when try delete wal")
		}
	} else {
		return errors.New("not support gc mode, please check the backupgc.conf")
	}

	return nil
}

func (gcs *GCSever) InitBackend() error {
	gcs.CapturePanic()

	var err error
	gcs.backend = fileSystem{}

	if gcs.conf.BackupStorageSpace.StorageType == "" {
		err = errors.New("StorageType is null. Please check the config.")
		gcs.logger.Printf("[ERROR] init backend failed : %s", err.Error())
		return err
	}

	gcs.backend.name = gcs.conf.BackupStorageSpace.StorageType

	plugin := configPath + "/" + gcs.backend.name + ".so"

	var m ModuleInfo
	var exports []string
	exports = append(exports, "ExportMapping")

	err = m.ModuleInit(plugin, exports)
	if err != nil {
		gcs.logger.Printf("[ERROR] main init module [%s] failed: %s\n", gcs.backend.name, err.Error())
		return err
	}
	gcs.logger.Printf("[INFO] main init module [%s] success\n", gcs.backend.name)
	_mapping, ok := m.Eat["ExportMapping"]
	if !ok {
		return fmt.Errorf("cannot find function %s.%s", plugin, "ExportMapping")
	}
	mapping, ok := _mapping.(func(interface{}, map[string]interface{}) (interface{}, error))
	if !ok {
		return fmt.Errorf("function %s.%s in imports must be func(interface{},map[string]interface{})(interface{}, error)", plugin, "ExportMapping")
	}

	entry, _ := mapping(nil, nil)
	gcs.backend.funcs, _ = entry.(map[string]func(interface{}, map[string]interface{}) (interface{}, error))

	initParam := make(map[string]interface{})

	confMap := make(map[string]interface{})
	confMap["Endpoint"] = gcs.conf.BackupStorageSpace.Locations.HTTP.Endpoint
	confMap["Path"] = gcs.conf.BackupStorageSpace.Locations.Local.Path
	content, err := json.MarshalIndent(confMap, "", "\t")

	gcs.logger.Printf("[INFO] init backend\n")
	initParam["conf"] = content
	ctx, err := gcs.backend.funcs["init"](nil, initParam)
	if err != nil {
		return err
	}

	gcs.backend.handle = ctx

	return err
}

func (gcs *GCSever) InitLog() error {
	if gcs.conf.Log.ForceStdout {
		gcs.logger = log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)
		gcs.logger.Printf("[INFO] main init log to stdout success\n")
	} else {
		compress := gcs.conf.Log.Compress
		fn := path.Join(gcs.conf.Log.Logdir, "backupgc.log")
		size := gcs.conf.Log.RotateSize
		count := gcs.conf.Log.MaxCounts
		age := gcs.conf.Log.MaxDays

		logger := &lumberjack.Logger{
			Filename:   fn,
			Compress:   compress,
			MaxSize:    size,
			MaxBackups: count,
			MaxAge:     age,
			LocalTime:  true,
		}
		gcs.logger = log.New(logger, "", log.LstdFlags|log.Lshortfile)
		gcs.logger.Printf("[INFO] main init log success, logpath: %s\n", fn)
	}
	return nil
}

func (gcs *GCSever) initConf() error {
	if configPath == "" {
		configPath = DefaultConfigPath
	}

	confpath := configPath + "/backupgc.conf"

	content, err := ioutil.ReadFile(confpath)
	if err != nil {
		return fmt.Errorf("read conf [%s] failed: %s", confpath, err.Error())
	}

	err = json.Unmarshal(content, &gcs.conf)
	if err != nil {
		return fmt.Errorf("parse conf [%s] failed: %s", confpath, err.Error())
	}

	return nil
}

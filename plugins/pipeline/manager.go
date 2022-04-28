package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ManagerCtx struct {
	callbackURL            string
	statusMap              sync.Map // map[Nodeip]RunningStatus
	lastUpdateTimeMap      sync.Map // map[jobid]int64
	lastBackedSizeMap      sync.Map
	lastStatusPrintTimeMap sync.Map
	recoveryMap            sync.Map
	recoveryStatusMap      sync.Map

	instance string // pg instance id
	user     string
	passwd   string
	ip       string
	port     int

	notify []byte
	logger *log.Logger
	stop   chan bool

	snapshot   string
	backupPlug string
	mode       string
	pbd        string
	metaDBConf CommonDBConf

	gbpsc      *GlobalBPSController
	backupjobs sync.Map

	statusPrintPeriod int
	serverPort        string
	netInterface      string

	dbsCallbackURL    string
	backupMachineList []string

	walSegSize int
}

type GlobalBPSController struct {
	MaxTotalBackupSpeed int
	MaxBackupSpeed      int
	isRunning           bool
	needUpdate          bool
}

type ManagerInitConf struct {
	BackupPlug          string       `json:"BackupPlug"`
	Mode                string       `json:"Mode"`
	Snapshot            string       `json:"Snapshot"` // snapshot script
	Pbd                 string       `json:"Pbd"`
	MetaDBConf          CommonDBConf `json:"MetaDBConf"`
	StatusPrintPeriod   int          `json:"StatusPrintPeriod"`
	DefaultServerPort   string       `json:"DefaultServerPort"`
	DefaultNetInterface string       `json:"DefaultNetInterface"`
	DBSCallbackURL      string       `json:"DBSCallbackURL"`
	BackupMachineList   []string     `json:"BackupMachineList"`
	WalSegSize          int          `json:"WalSegSize"`
}

func PluginInit(ctx interface{}) (interface{}, error) {
	m, ok := ctx.(map[string]interface{})
	if !ok {
		return nil, errors.New("invalid init ctx")
	}

	_logger, ok := m["logger"]
	if !ok {
		return nil, errors.New("ctx.conf miss")
	}
	logger, ok := _logger.(*log.Logger)
	if !ok {
		return nil, errors.New("ctx.conf must be *log.Logger")
	}

	_conf, ok := m["conf"]
	if !ok {
		return nil, errors.New("ctx.conf miss")
	}
	conf, ok := _conf.([]byte)
	if !ok {
		return nil, errors.New("ctx.conf must be []byte")
	}

	var initConf ManagerInitConf
	err := json.Unmarshal(conf, &initConf)
	if err != nil {
		return nil, errors.New("parse plugin conf failed")
	}

	defaultServerPort := DefaultServerPort
	if initConf.DefaultServerPort != "" {
		defaultServerPort = initConf.DefaultServerPort
	}

	defaultNetInterface := DefaultNetInterface
	if initConf.DefaultNetInterface != "" {
		defaultNetInterface = initConf.DefaultNetInterface
	}

	return &ManagerCtx{
		statusMap:              sync.Map{},
		lastUpdateTimeMap:      sync.Map{},
		lastStatusPrintTimeMap: sync.Map{},
		mode:                   initConf.Mode,
		snapshot:               initConf.Snapshot,
		pbd:                    initConf.Pbd,
		metaDBConf:             initConf.MetaDBConf,
		backupPlug:             initConf.BackupPlug,
		logger:                 logger,
		backupjobs:             sync.Map{},
		gbpsc:                  &GlobalBPSController{},
		statusPrintPeriod:      initConf.StatusPrintPeriod,
		serverPort:             defaultServerPort,
		netInterface:           defaultNetInterface,
		dbsCallbackURL:         initConf.DBSCallbackURL,
		backupMachineList:      initConf.BackupMachineList,
		walSegSize:             initConf.WalSegSize,
	}, nil
}

func PluginRun(ctx interface{}, param interface{}) error {
	_, ok := ctx.(*ManagerCtx)
	if !ok {
		return errors.New("ctx must be nil or *ManagerCtx")
	}

	return nil
}

func PluginExit(ctx interface{}) error {
	_, ok := ctx.(*ManagerCtx)
	if !ok {
		return errors.New("ctx must be nil or *ManagerCtx")
	}
	return nil
}

type QueryRequest struct {
	BackupJobID string `json:"BackupJobID"`
}

func DeleteBackupCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	return nil, nil
}

func CheckBackupCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	return nil, nil
}

func recordFullBackupToMultiRemoteDB(mCtx *ManagerCtx, rs *RunningStatus) error {
	bkdbInfo, err := initPgsqlDBInfo(&rs.PgDBConf)
	if err != nil {
		return err
	}
	err = recordFullBackupToRemoteDB(mCtx, bkdbInfo, rs)
	if err != nil {
		return err
	}
	defer bkdbInfo.db.Close()

	if mCtx.metaDBConf.DBType != "" {
		var dbconf CommonDBConf
		dbconf.Endpoint = mCtx.metaDBConf.Endpoint
		dbconf.Port = mCtx.metaDBConf.Port
		dbconf.Username = mCtx.metaDBConf.Username
		dbconf.Password = mCtx.metaDBConf.Password
		dbconf.Database = mCtx.metaDBConf.Database
		dbconf.ApplicationName = "default"

		if mCtx.metaDBConf.DBType == "pgsql" {
			mtdbInfo, err := initPgsqlDBInfo(&dbconf)
			if err != nil {
				return err
			}
			defer mtdbInfo.db.Close()
			err = recordFullBackupToRemoteDB(mCtx, mtdbInfo, rs)
			if err != nil {
				return err
			}
		} else if mCtx.metaDBConf.DBType == "mysql" {
			mtdbInfo, err := initMysqlDBInfo(&dbconf)
			if err != nil {
				return err
			}
			defer mtdbInfo.db.Close()
			err = recordFullBackupToRemoteDB(mCtx, mtdbInfo, rs)
			if err != nil {
				return err
			}
		} else {
			return errors.New("Not support db type")
		}
	}

	return nil
}

func recordFullBackupToRemoteDB(mCtx *ManagerCtx, dbInfo *DBInfo, rs *RunningStatus) error {
	sqlStatement := "CREATE TABLE IF NOT EXISTS backup_meta (" +
		"instanceid varchar(128) NOT NULL, " +
		"backupid varchar(128) NOT NULL, " +
		"backupjobid varchar(128) NOT NULL, " +
		"file varchar(128) NOT NULL, " +
		"backuptype varchar(16) NOT NULL, " +
		"starttime BIGINT NOT NULL DEFAULT '0', " +
		"endtime BIGINT NOT NULL DEFAULT '0', " +
		"location varchar(1024) NOT NULL, " +
		"status varchar(64) NOT NULL, " +
		"primary key (instanceid, backupid, file)" +
		");"
	rows, err := queryDB(dbInfo, sqlStatement)
	if err != nil {
		return err
	}
	defer rows.Close()

	var sql string
	if dbInfo.dbType == "pgsql" {
		sql = `
        INSERT INTO backup_meta (instanceid, backupid, backupjobid, file, backuptype, starttime, endtime, location, status)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	} else if dbInfo.dbType == "mysql" {
		sql = `
        INSERT INTO backup_meta (instanceid, backupid, backupjobid, file, backuptype, starttime, endtime, location, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	} else {
		return errors.New("Not support db type")
	}

	backuptype := "full"
	if rs.UseBlock {
		backuptype = "block"
	}
	_, err = dbInfo.db.Exec(sql, rs.InstanceID, rs.BackupID, rs.BackupJobID, rs.ID, backuptype, rs.StartTime, rs.EndTime, rs.Location, rs.Stage)
	if err != nil {
		return err
	}

	return nil
}

func handleDBSCallback(mCtx *ManagerCtx, rs *RunningStatus) error {
	var err error

	if rs.Action == "backup" && rs.Type == "Incremental" && rs.Stage == "Running" && rs.CurLogFile.FileName == "" {
		return nil
	}

	forcePost := false

	statusReq := make(map[string]interface{})
	statusReq["InstanceId"] = rs.InstanceID
	/*
	 * According to dbs, BackupID should be same since it represent backup plan, therefore, use origin InstanceID instead.
	 * BackupJobID should be different when backup, therefore, use origin BackupID instead.
	 */
	statusReq["BackupPlanId"] = rs.InstanceID
	statusReq["BackupJobId"] = rs.BackupID
	statusReq["Status"] = rs.Stage

	dbsu, err := url.ParseRequestURI(rs.Location)
	if err != nil {
		return err
	}
	if dbsu.Path != "" {
		path, _ := ParsePath(dbsu.Path)
		statusReq["Path"] = "/" + path + "/" + rs.InstanceID + "/" + rs.BackupID
	} else {
		statusReq["Path"] = "/" + rs.InstanceID + "/" + rs.BackupID
	}

	process := 0
	if rs.Action == "backup" && rs.Type == "Full" {
		if rs.BackupSize > 0 {
			process = int(rs.BackedSize * 100 / rs.BackupSize)
		}

		if rs.Stage == "Finished" {
			process = 100
		}

		statusReq["StartTime"] = rs.StartTime
		statusReq["EndTime"] = rs.EndTime
		statusReq["StartOffset"] = 0
		statusReq["EndOffset"] = 0
		statusReq["Process"] = process
		statusReq["Mbytes"] = rs.BackupSize

		if rs.Stage == "Finished" || rs.Stage == "Failed" {
			forcePost = true
		}
	}

	if rs.Action == "backup" && rs.Type == "Incremental" && rs.Stage == "Running" {
		wals := make(map[string]interface{})
		wals["LogFile"] = rs.CurLogFile

		statusReq["NewLogFiles"] = wals
		statusReq["Status"] = "Finished"
		forcePost = true
	}

	content, err := json.MarshalIndent(&statusReq, "", "\t")
	if err != nil {
		return err
	}

	r := bytes.NewReader(content)
	var resp *http.Response
	u, err := url.Parse(mCtx.dbsCallbackURL)
	if err != nil {
		mCtx.logger.Printf("[ERROR] manager parse callbackURL failed: %s\n", err.Error())
		return err
	}

	if rs.Action == "backup" && rs.Type == "Full" {
		u.Path = path.Join(u.Path, "UpdateFullBackupStatus")
		mCtx.logger.Printf("[INFO] manager send full backup status [process:%d] to dbs %s\n", process, u.String())
		resp, err = RobustPost(mCtx, u.String(), "application/json", r, forcePost)
	} else if rs.Action == "backup" && rs.Type == "Incremental" {
		u.Path = path.Join(u.Path, "UpdateIncrementalBackupStatus")
		mCtx.logger.Printf("[INFO] manager send wal backup status %s to dbs %s\n", string(content), u.String())
		resp, err = RobustPost(mCtx, u.String(), "application/json", r, forcePost)
	} else {
		return nil
	}

	if resp != nil {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		var respData DBSResponseData
		err = json.Unmarshal(data, &respData)
		if err != nil {
			return err
		}

		if respData.Code != 200 && forcePost {
			err = errors.New("get error from dbs: " + respData.Error)
		}
	}

	return err
}

func DescribeCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	mCtx, ok := ctx.(*ManagerCtx)
	if !ok {
		err := errors.New(`{"error": "ctx must be *ManagerCtx", "code": 1002}`)
		return err.Error(), err
	}

	_req, ok := param["req"]
	if !ok {
		err := errors.New(`{"error": "param.req miss", "code": 1002}`)
		return err.Error(), err
	}
	data, ok := _req.([]byte)
	if !ok {
		err := errors.New(`{"error": "param.req must be []byte", "code": 1002}`)
		return err.Error(), err
	}

	var req DescribeRequest
	err := json.Unmarshal(data, &req)
	if err != nil {
		e := fmt.Errorf(`{"error": "invalid Describe request, parse requst failed: %s", "code": 1002}`, err.Error())
		mCtx.logger.Printf("[ERROR] manager parse Describe request failed\n")
		return e.Error(), e
	}

	var ret string
	if req.BackupType == "Recovery" {
		id := req.InstanceID
		_recoveryStatus, ok := mCtx.recoveryStatusMap.Load(id)
		if !ok {
			e := fmt.Errorf(`{"error": "not found instance id: %s in recovery", "code": 1002}`, id)
			mCtx.logger.Printf("[ERROR] manager not found instanceid %s in recovery status map\n", id)
			return e.Error(), e
		}
		recoveryStatus := _recoveryStatus.(RecoveryStatus)

		reportReq := make(map[string]interface{})

		statusReq := make(map[string]interface{})
		statusReq["InstanceID"] = recoveryStatus.InstanceID
		statusReq["BackupID"] = recoveryStatus.BackupID
		statusReq["BackupJobID"] = recoveryStatus.BackupJobID
		statusReq["Status"] = recoveryStatus.Status
		statusReq["Process"] = recoveryStatus.Process
		statusReq["WalsNum"] = recoveryStatus.WalsNum
		statusReq["RecoverySize"] = recoveryStatus.RecoverySize
		statusReq["RecoveredSize"] = recoveryStatus.RecoveredSize

		reportReq["code"] = 0
		if recoveryStatus.Status == "Failed" {
			reportReq["code"] = 1003
			reportReq["error"] = recoveryStatus.Error
		}
		reportReq["data"] = statusReq

		content, err := json.MarshalIndent(&reportReq, "", "\t")
		if err != nil {
			e := fmt.Errorf(`{"error": "marshal map failed: %s", "code": 1003}`, err.Error())
			mCtx.logger.Printf("[ERROR] marshal map failed: %s\n", err.Error())
			return e.Error(), e
		}

		ret = string(content)
	} else {
		e := fmt.Errorf(`{"error": "not support backup type: %s", "code": 1002}`, req.BackupType)
		mCtx.logger.Printf("[ERROR] not support backup type: %s\n", req.BackupType)
		return e.Error(), e
	}

	return ret, nil
}

func UpdateRecoveryStatus(mCtx *ManagerCtx, rs RunningStatus) {
	id := rs.InstanceID
	_recoveryStatus, ok := mCtx.recoveryStatusMap.Load(id)
	if !ok {
		mCtx.logger.Printf("[ERROR] manager not found instanceid %s in recovery status map", id)
		return
	}

	recoveryStatus := _recoveryStatus.(RecoveryStatus)

	/*
	 * TODO block backup
	 */
	if recoveryStatus.RecoverySize == 0 && recoveryStatus.BackupID == rs.BackupID && rs.BackupSize != 0 {
		recoveryStatus.RecoverySize = rs.BackupSize + int64(recoveryStatus.WalsNum * mCtx.walSegSize)
	}

	recoveryStatus.BackupID = rs.BackupID

	if rs.Type == "Full" {
		recoveryStatus.RecoveredSize = rs.BackedSize
		if rs.Stage == "Finished" {
			recoveryStatus.FullRecoveredSize = recoveryStatus.RecoveredSize
		}
	} else if rs.Type == "Incremental" {
		recoveryStatus.RecoveredSize = rs.BackedSize + recoveryStatus.FullRecoveredSize
	}

	process := 0
	if recoveryStatus.RecoverySize > 0 {
		process = int(recoveryStatus.RecoveredSize * 100 / recoveryStatus.RecoverySize)
	}

	recoveryStatus.Status = rs.Stage
	recoveryStatus.Error = rs.Error

	if rs.Stage == "Finished" {
		if rs.Type == "Incremental" || recoveryStatus.WalsNum == 0 {

		} else {
			recoveryStatus.Status = "Running"
		}
	}

	if rs.Stage == "Finished" || process >= 100 {
		process = 100
	}

	recoveryStatus.Process = process

	mCtx.recoveryStatusMap.Store(id, recoveryStatus)
}

func BackupStatus(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	mCtx, ok := ctx.(*ManagerCtx)
	if !ok {
		err := errors.New("ctx must be nil or *ManagerCtx")
		return err.Error(), err
	}

	_req, ok := param["req"]
	if !ok {
		err := errors.New(`{"error": "param.req miss", "code":1002}`)
		return err.Error(), err
	}
	data, ok := _req.([]byte)
	if !ok {
		err := errors.New(`{"error": "param.req must be []byte", "code":1002}`)
		return err.Error(), err
	}

	var req RunningStatus
	err := json.Unmarshal(data, &req)
	if err != nil {
		err := fmt.Errorf(`{"error": "invalid backup status request, parse requst failed: %s", "code": 1002}`, err.Error())
		return err.Error(), err
	}

	_query, ok := param["query"]
	if !ok {
		err := errors.New(`{"error": "param.query miss", "code":1002}`)
		return err.Error(), err
	}
	query, ok := _query.(url.Values)
	if !ok {
		err := errors.New(`{"error": "param.query must be url.Values", "code":1002}`)
		return err.Error(), err
	}

	callbackURL := ""
	if len(req.Callback) > 0 {
		callbackURL = req.Callback
	}

	if req.Action == "restore" {
		UpdateRecoveryStatus(mCtx, req)
	}

	_, ok = query["multistage"]
	if ok {
		return MultiStageRecoveryCallback(ctx, param)
	}

	current := time.Now().Unix()
	lastUpdateTime, ok := mCtx.lastUpdateTimeMap.Load(req.BackupJobID)
	if !ok {
		lastUpdateTime = 0
	}

	id := req.InstanceID + req.BackupID + req.BackupJobID
	mCtx.lastUpdateTimeMap.Store(req.BackupJobID, current)
	mCtx.statusMap.Store(req.Node+"."+id, req)

	total := req.AllFilesCount
	action := req.Action
	backuptype := req.Type

	var backupSize, backedSize int64
	backupSize = 0
	backedSize = 0
	upload := 0
	process := 0
	download := 0
	stage := req.Stage

	cleanMap := false
	mCtx.statusMap.Range(func(_, v interface{}) bool {
		status := v.(RunningStatus)
		if status.BackupJobID == req.BackupJobID {
			download += int(status.Download)
			upload += status.Upload
			backupSize += status.BackupSize
			backedSize += status.BackedSize
			if status.Stage == "Failed" {
				stage = "Failed"
			}
		}
		return true
	})

	lastBackedSize, ok := mCtx.lastBackedSizeMap.Load(req.BackupJobID)
	if !ok {
		lastBackedSize = 0
	}
	mCtx.lastBackedSizeMap.Store(req.BackupJobID, backedSize)
	var backupSpeed int64
	if lastUpdateTime != 0 {
		gap := int64(current) - lastUpdateTime.(int64)
		if gap > 0 {
			backupSpeed = (backedSize - lastBackedSize.(int64)) / gap
		}
	}

	if upload != 0 {
		process = upload * 100 / total
	}
	if download != 0 {
		process = download * 100 / total
	}

	if backuptype == "Full" && action == "backup" {
		if backupSize > 0 {
			process = int(backedSize * 100 / backupSize)
		}
	}

	if stage == "Finished" && backuptype == "Full" && action == "backup" {
		id := req.InstanceID + req.BackupID + req.BackupJobID
		mCtx.backupjobs.Delete(id)
		mCtx.gbpsc.needUpdate = true

		err = recordFullBackupToMultiRemoteDB(mCtx, &req)
		if err != nil {
			stage = "Failed"
			req.Error = err.Error()
			mCtx.logger.Printf("[ERROR] manager record backup info to remote db failed: %s", err.Error())
		}
	}

	if stage == "Finished" {
		process = 100
	}

	forcePost := false
	if stage == "Finished" || stage == "Failed" {
		cleanMap = true
		forcePost = true
	}

	if cleanMap {
		mCtx.statusMap.Range(func(key interface{}, value interface{}) bool {
			status := value.(RunningStatus)
			if status.BackupJobID == req.BackupJobID {
				mCtx.statusMap.Delete(key)
			}
			return true
		})
		mCtx.lastUpdateTimeMap.Delete(req.BackupJobID)
		mCtx.lastBackedSizeMap.Delete(req.BackupJobID)
		mCtx.lastStatusPrintTimeMap.Delete(req.BackupJobID)

		id := req.InstanceID + req.BackupID + req.BackupJobID
		mCtx.backupjobs.Delete(id)
	}
	if callbackURL == "" {
		mCtx.logger.Printf("[WARNING] manager callbackURL is null\n")
		return `{"error": null, "code": 0}`, nil
	}

	reportReq := make(map[string]interface{})

	statusReq := make(map[string]interface{})
	statusReq["InstanceID"] = req.InstanceID
	statusReq["BackupID"] = req.BackupID
	statusReq["BackupJobID"] = req.BackupJobID
	statusReq["Status"] = stage
	if action == "backup" && backuptype == "Full" {
		statusReq["StartTime"] = req.StartTime
		statusReq["EndTime"] = req.EndTime
		statusReq["StartOffset"] = 0
		statusReq["EndOffset"] = 0
	}
	if action == "restore" || action == "backup" && backuptype == "Full" {
		statusReq["Process"] = process
	}
	if backuptype == "Full" && (action == "backup" || action == "restore") {
		statusReq["BackupSize"] = backupSize
		statusReq["BackedSize"] = backedSize
		statusReq["BackupSpeed"] = backupSpeed
	}
	if action == "backup" && backuptype == "Incremental" && stage == "Running" {
		statusReq["NewLogFiles"] = req.CurLogFile
	}

	/*
	 * if dbs callback is needed, after dbs callback, we should record that whether dbs callbackup is successfully
	 */
	if req.Backend == "dbs" && mCtx.dbsCallbackURL != "" && action == "backup" {
		err = handleDBSCallback(mCtx, &req)
		if err != nil {
			mCtx.logger.Printf("[ERROR] dbs callback failed: %s\n", err.Error())
			stage = "Failed"
			statusReq["Status"] = stage
			if req.Error == "" {
				req.Error = err.Error()
			} else {
				req.Error = req.Error + ", " + err.Error()
			}
		}
	}

	reportReq["code"] = 0
	if stage == "Failed" {
		reportReq["code"] = 1
		reportReq["error"] = req.Error
	}
	reportReq["data"] = statusReq

	content, err := json.MarshalIndent(&reportReq, "", "\t")
	if err != nil {
		return nil, err
	}

	needPrint := true
	if backuptype == "Incremental" && stage == "Running" {
		lastStatusPrintTime, ok := mCtx.lastStatusPrintTimeMap.Load(req.BackupJobID)
		if ok {
			gap := current - lastStatusPrintTime.(int64)
			if int(gap) >= mCtx.statusPrintPeriod {
				mCtx.lastStatusPrintTimeMap.Store(req.BackupJobID, current)
			} else {
				needPrint = false
			}
		} else {
			mCtx.lastStatusPrintTimeMap.Store(req.BackupJobID, current)
		}
	}
	if needPrint {
		mCtx.logger.Printf("[INFO] manager callback status:\n%s\n", string(content))
	}

	r := bytes.NewReader(content)
	var resp *http.Response
	u, err := url.Parse(callbackURL)
	if err != nil {
		e := fmt.Errorf(`{"error": "%s", "code": 1002}`, err.Error())
		mCtx.logger.Printf("[ERROR] manager parse callbackURL failed: %s\n", err.Error())
		return e.Error(), e
	}

	if action == "backup" && backuptype == "Full" {
		u.Path = path.Join(u.Path, "UpdateFullBackupStatus")
		mCtx.logger.Printf("[INFO] manager send backup status [process:%d] to %s\n", process, u.String())
		resp, err = RobustPost(mCtx, u.String(), "application/json", r, forcePost)
	} else if (action == "backup" || action == "stop") && backuptype == "Incremental" {
		u.Path = path.Join(u.Path, "UpdateIncrementalBackupStatus")
		if needPrint {
			mCtx.logger.Printf("[INFO] manager send backup status [process:%d] to %s\n", process, u.String())
		}
		resp, err = RobustPost(mCtx, u.String(), "application/json", r, forcePost)
	} else if action == "restore" {
		u.Path = path.Join(u.Path, "UpdateRecoveryStatus")
		mCtx.logger.Printf("[INFO] manager send recovery status [process:%d] to %s\n", process, u.String())
		resp, err = RobustPost(mCtx, u.String(), "application/json", r, forcePost)
	}
	if err == nil {
		if resp != nil {
			err := HandleResponse(mCtx, resp, req)
			if err != nil {
				mCtx.logger.Printf("[ERROR] manager handle response error: %s\n", err.Error())
			}
			resp.Body.Close()
		}
		return `{"error": null, "code": 0}`, nil
	}
	e := fmt.Errorf(`{"error": "%s", "code": 1002}`, err.Error())
	return e.Error(), e
}

func HandleResponse(mCtx *ManagerCtx, resp *http.Response, req RunningStatus) error {
	if resp == nil {
		return errors.New("resp is nil")
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var respData ResponseData
	err = json.Unmarshal(data, &respData)
	if err != nil {
		return err
	}

	mCtx.logger.Printf("[INFO] manager recieve response: %s\n", string(data))

	/* Accoding to agreement with PolarDBStack, when return the following code, backup agent should stop the full backup */
	if respData.Code == BackupExecuteNotExist || respData.Code == DBClusterNotExist {
		if req.Action == "backup" && req.Type == "Full" {

			var stopReq StopBackupRequest

			id := req.InstanceID + req.BackupID + req.BackupJobID
			if job, ok := mCtx.backupjobs.Load(id); ok {
				jobreq := job.(BackupRequest)

				stopReq.BackupMachineList = jobreq.BackupMachineList
				stopReq.InstanceID = jobreq.InstanceID
				stopReq.BackupID = jobreq.BackupID
				stopReq.BackupJobID = jobreq.BackupJobID
				stopReq.BackupType = jobreq.BackupType
				stopReq.Filesystem = jobreq.Filesystem

				data, err = json.Marshal(stopReq)
				if err != nil {
					return err
				}

				param := make(map[string]interface{})
				param["req"] = data
				mCtx.logger.Printf("[INFO] receive error code: %d from db stack, manager stop backup\n", respData.Code)
				_, err = StopCallback(mCtx, param)
				if err != nil {
					mCtx.logger.Printf("[ERROR] manager stop backup error: %s\n", err.Error())
					return err
				}
			} else {
				mCtx.logger.Printf("[WARNING] try stop backup, not found specify backup job by id: %s in map\n", id)
			}
		} else if req.Action == "restore" {
			var stopReq StopBackupRequest

			id := req.InstanceID + req.BackupID + req.BackupJobID
			if job, ok := mCtx.recoveryMap.Load(id); ok {
				jobreq := job.(RecoveryRequest)

				stopReq.BackupMachineList = jobreq.BackupMachineList
				stopReq.InstanceID = req.InstanceID
				stopReq.BackupID = req.BackupID
				stopReq.BackupJobID = req.BackupJobID
				stopReq.Filesystem = jobreq.Filesystem

				// TODO block backup
				if jobreq.Stage == "full" || jobreq.Stage == "fullonly" {
					stopReq.BackupType = "Full"
				} else {
					stopReq.BackupType = "Incremental"
				}

				data, err = json.Marshal(stopReq)
				if err != nil {
					return err
				}

				param := make(map[string]interface{})
				param["req"] = data
				mCtx.logger.Printf("[INFO] receive error code: %d from db stack, stage is %s, manager stop recovery\n", respData.Code, jobreq.Stage)
				_, err = StopCallback(mCtx, param)
				if err != nil {
					mCtx.logger.Printf("[ERROR] manager stop recovery error: %s\n", err.Error())
					return err
				}
			} else {
				mCtx.logger.Printf("[WARNING] try stop recovery, but not found specify recovery job by id: %s in map\n", id)
			}
		}
	}

	return nil
}

func RobustPost(mctx *ManagerCtx, url string, mode string, r io.Reader, forcePost bool) (*http.Response, error) {
	if !forcePost {
		return http.Post(url, mode, r)
	}

	var resp *http.Response
	tryMax := 3
	for i := 0; i < tryMax; i++ {
		resp, err := http.Post(url, mode, r)
		if err != nil {
			time.Sleep(5 * time.Second)
		} else {
			return resp, err
		}
	}

	mctx.logger.Printf("[ERROR] manager try post 3 times to %s, failed", url)
	return resp, errors.New("try post 3 times to report status, failed")
}

func BroadcastRequest(mctx *ManagerCtx, plugin string, nodes []string, p string, data []byte) error {
	for _, node := range nodes {
		r := bytes.NewReader(data)
		url := node + "/" + plugin + p
		if !strings.HasPrefix(node, "http") {
			url = "http://" + url
		}

		resp, err := http.Post(url, "application/json", r)
		if err != nil {
			mctx.logger.Printf("[ERROR] manager request [%s]  failed: %s\n", url, err.Error())
			return errors.New(`{"error": "backup service error", "code": 1}`)
		}
		defer resp.Body.Close()
		mctx.logger.Printf("[INFO] manager request [%s] success\n", url)

		respContent, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			mctx.logger.Printf("[ERROR] manager read [%s] response failed: %s\n", url, err.Error())
			return errors.New(`{"error": "backup service error", "code": 1}`)
		}
		var respInfo ResponseData
		err = json.Unmarshal(respContent, &respInfo)
		if err != nil {
			mctx.logger.Printf("[ERROR] manager [%s] response parse failed: %s\n", url, err.Error())
			return errors.New(`{"error": "backup service error", "code": 1}`)
		}
		if respInfo.Code != 0 {
			mctx.logger.Printf("[ERROR] manager [%s] execute failed: %s\n", url, respInfo.Error)
			return errors.New(string(respContent))
		}
	}
	mctx.logger.Printf("[INFO] manager request [%s] to [%s] success\n", p, strings.Join(nodes, ","))
	return nil
}

//////////////////////////////////////////////////////////////////////////////////////////////////////

func RenameWalCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	mCtx, ok := ctx.(*ManagerCtx)
	if !ok {
		err := errors.New(`{"error": "ctx must be *ManagerCtx", "code": 1002}`)
		return err.Error(), err
	}

	_req, ok := param["req"]
	if !ok {
		err := errors.New(`{"error": "param.req miss", "code": 1002}`)
		return err.Error(), err
	}
	data, ok := _req.([]byte)
	if !ok {
		err := errors.New(`{"error": "param.req must be []byte", "code": 1002}`)
		return err.Error(), err
	}

	var req WalHelperRequest
	err := json.Unmarshal(data, &req)
	if err != nil {
		mCtx.logger.Printf("[ERROR] manager parse stop request failed\n")
		e := fmt.Errorf(`{"error": "invalid rename wal request, parse requst failed: %s", "code": 1002}`, err.Error())
		return e.Error(), e
	}

	data, err = json.Marshal(req)
	if err != nil {
		err := fmt.Errorf(`{"error": "%s", code: 1002}`, err.Error())
		return err.Error(), err
	}

	mCtx.logger.Printf("[INFO] manager send command: %s to nodes: %s\n", req.Action, strings.Join(mCtx.backupMachineList, ","))

	nodes := mCtx.backupMachineList
	plugin := "wal"

	err = BroadcastRequest(mCtx, plugin, nodes, "/RenameWal", data)
	if err != nil {
		return err.Error(), err
	}

	return `{"error": null, "code": 0}`, nil
}

func BlockHelperCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	mCtx, ok := ctx.(*ManagerCtx)
	if !ok {
		err := errors.New(`{"error": "ctx must be *ManagerCtx", "code": 1002}`)
		return err.Error(), err
	}

	_req, ok := param["req"]
	if !ok {
		err := errors.New(`{"error": "param.req miss", "code": 1002}`)
		return err.Error(), err
	}
	data, ok := _req.([]byte)
	if !ok {
		err := errors.New(`{"error": "param.req must be []byte", "code": 1002}`)
		return err.Error(), err
	}

	var whReq WalHelperRequest
	err := json.Unmarshal(data, &whReq)
	if err != nil {
		mCtx.logger.Printf("[ERROR] manager parse stop request failed\n")
		e := fmt.Errorf(`{"error": "invalid rename wal request, parse requst failed: %s", "code": 1002}`, err.Error())
		return e.Error(), e
	}

	var nodes []string
	instanceid := whReq.InstanceID
	mCtx.backupjobs.Range(func(k, v interface{}) bool {
		req := v.(BackupRequest)
		if req.BackupType == "Incremental" && req.InstanceID == instanceid {
			nodes = req.BackupMachineList
		}
		return true
	})
	// data, err = json.Marshal(req)
	// if err != nil {
	// 	err := fmt.Errorf(`{"error": "%s", code: 1002}`, err.Error())
	// 	return err.Error(), err
	// }

	mCtx.logger.Printf("[INFO] manager send block helper command to nodes: %s\n", strings.Join(nodes, ","))
	plugin := "wal"

	err = BroadcastRequest(mCtx, plugin, nodes, "/BlockHelper", data)
	if err != nil {
		return err.Error(), err
	}

	return `{"error": null, "code": 0}`, nil
}

func StopCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	mCtx, ok := ctx.(*ManagerCtx)
	if !ok {
		err := errors.New(`{"error": "ctx must be *ManagerCtx", "code": 1002}`)
		return err.Error(), err
	}

	_req, ok := param["req"]
	if !ok {
		err := errors.New(`{"error": "param.req miss", "code": 1002}`)
		return err.Error(), err
	}
	data, ok := _req.([]byte)
	if !ok {
		err := errors.New(`{"error": "param.req must be []byte", "code": 1002}`)
		return err.Error(), err
	}

	var req StopBackupRequest
	err := json.Unmarshal(data, &req)
	if err != nil {
		mCtx.logger.Printf("[ERROR] manager parse stop request failed\n")
		e := fmt.Errorf(`{"error": "invalid stop request, parse requst failed: %s", "code": 1002}`, err.Error())
		return e.Error(), e
	}

	if mCtx.metaDBConf.DBType != "" {
		req.MetaAccount.Endpoint = mCtx.metaDBConf.Endpoint
		req.MetaAccount.Port = mCtx.metaDBConf.Port
		req.MetaAccount.User = mCtx.metaDBConf.Username
		req.MetaAccount.Password = mCtx.metaDBConf.Password
		req.MetaAccount.Database = mCtx.metaDBConf.Database
		req.MetaAccount.DBType = mCtx.metaDBConf.DBType
	}

	data, err = json.Marshal(req)
	if err != nil {
		err := fmt.Errorf(`{"error": "%s", "code": 1002}`, err.Error())
		return err.Error(), err
	}

	mCtx.logger.Printf("[INFO] manager send stop command to nodes: %s\n", strings.Join(req.BackupMachineList, ","))

	nodes := req.BackupMachineList
	plugin := ""
	if req.BackupType == "Incremental" {
		plugin = "increpipeline"
	} else if req.BackupType == "Full" {
		if req.Filesystem == "pfs.file" {
			plugin = "pgpipeline"
		} else if req.Filesystem == "pfs.chunk" {
			plugin = "pipeline"
		}
	}
	if plugin == "" {
		mCtx.logger.Printf("[ERROR] manager can not decide which type of plugin\n")
		e := fmt.Errorf(`{"error": "invalid stop request, manager can not decide which type of plugin", "code": 1002}`)
		return e.Error(), e
	}

	nodes, err = RedirectNodes(mCtx, plugin, nodes)
	if err != nil {
		mCtx.logger.Printf("[ERROR] manager redirect stop nodes failed\n")
		e := fmt.Errorf(`{"error": "redirect stop nodes failed", "code": 1002}`)
		return e.Error(), e
	}

	err = BroadcastRequest(mCtx, plugin, nodes, "/Stop", data)
	if err != nil {
		return err.Error(), err
	} else {
		id := req.InstanceID + req.BackupID + req.BackupJobID
		if _, ok := mCtx.backupjobs.Load(id); ok {
			mCtx.backupjobs.Delete(id)
			mCtx.gbpsc.needUpdate = true
		} else if _, ok := mCtx.recoveryMap.Load(id); ok {
			mCtx.recoveryMap.Delete(id)
		}

		return `{"error": null, "code": 0}`, nil
	}
}

func RedirectNodes(mCtx *ManagerCtx, plugin string, nodes []string) ([]string, error) {
	mCtx.logger.Printf("[INFO] redirect node for stop backup\n")
	if plugin != "increpipeline" {
		return nodes, nil
	}

	if len(nodes) != 1 {
		return nodes, errors.New("illegal number of nodes to stop backup")
	}

	isNodeValid := false
	tryMax := 3
	for i := 0; i < tryMax; i++ {
		var nulldata []byte
		r := bytes.NewReader(nulldata)
		node := nodes[0]
		url := node + "/" + plugin + "/HeartBeat"
		if !strings.HasPrefix(node, "http") {
			url = "http://" + url
		}

		resp, err := http.Post(url, "application/json", r)
		if err == nil {
			mCtx.logger.Printf("[INFO] post %s valid\n", url)
			isNodeValid = true
			resp.Body.Close()
			break
		}
		time.Sleep(1 * time.Second)
	}

	if isNodeValid {
		mCtx.logger.Printf("[INFO] node: %s is valid\n", nodes[0])
		return nodes, nil
	}

	ip, err := getHostIp(mCtx)
	if err != nil {
		mCtx.logger.Printf("[ERROR] can not get legal host ip: %s\n", err.Error())
		return nodes, errors.New("can not get legal host ip:" + err.Error())
	}

	mCtx.logger.Printf("[INFO] node: %s is invalid when try %d times, change to self node: %s\n", nodes[0], tryMax, ip)

	nodes[0] = ip + ":" + mCtx.serverPort
	return nodes, nil
}

func getHostIp(mCtx *ManagerCtx) (string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		mCtx.logger.Printf("Failed to get net interface %s", err.Error())
		return "", errors.New("Failed to get net interface " + err.Error())
	}
	matchInterface := mCtx.netInterface

	for _, netInterface := range interfaces {
		if netInterface.Name == matchInterface {
			addrs, _ := netInterface.Addrs()
			for _, addr := range addrs {
				mCtx.logger.Printf("Addr %s:%s", addr.String(), addr.Network())
				if strings.Contains(addr.String(), ".") {
					return strings.Split(addr.String(), "/")[0], nil
				}
			}
		}
	}
	return "", errors.New("Can not find the host ip")
}

func CalculateFullbackupByFile(mCtx *ManagerCtx, storage *BackupStorageSpace, funcs *sync.Map, recoveryTime int64, instanceid string) (BackupMetaInfo, error) {
	var fullbackup BackupMetaInfo

	fullbackups, err := GetFullBackupSets(mCtx, storage, funcs, instanceid)
	if err != nil {
		return fullbackup, err
	}

	var maxEndtime int64
	maxEndtime = 0

	for _, fb := range fullbackups {
		if !fb.IsBlock && fb.EndTime < recoveryTime && fb.EndTime > maxEndtime {
			fullbackup = fb
			maxEndtime = fb.EndTime
		}
	}

	if maxEndtime == 0 {
		return fullbackup, errors.New("Not found valid full backup set")
	}

	return fullbackup, nil
}

func CalculateBlockbackupsByFile(mCtx *ManagerCtx, storage *BackupStorageSpace, funcs *sync.Map, prevEndTime int64, recoveryTime int64, instanceid string) ([]BackupMetaInfo, error) {
	var blockbackups []BackupMetaInfo

	fullbackups, err := GetFullBackupSets(mCtx, storage, funcs, instanceid)
	if err != nil {
		return blockbackups, err
	}

	var lastStarttime int64
	lastStarttime = 0
	for _, fb := range fullbackups {
		if fb.IsBlock && fb.StartTime > prevEndTime && fb.EndTime < recoveryTime {
			blockbackups = append(blockbackups, fb)

			/*
			* check if block backups are order preserving since it needed by recovery
			*/
			if lastStarttime == 0 {
				lastStarttime = fb.StartTime
			} else if lastStarttime >= fb.StartTime {
				return blockbackups, errors.New("timing of block backups in meta is illegal since start time is not order preserving")
			} else {
				lastStarttime = fb.StartTime
			}
		}
	}

	return blockbackups, nil
}

func FindSpecifyFullbackupByFile(mCtx *ManagerCtx, storage *BackupStorageSpace, funcs *sync.Map, instanceid string, backupid string) (BackupMetaInfo, error) {
	var fullbackup BackupMetaInfo

	fullbackups, err := GetFullBackupSets(mCtx, storage, funcs, instanceid)
	if err != nil {
		return fullbackup, err
	}

	for _, fb := range fullbackups {
		if fb.BackupID == backupid {
			fullbackup = fb
			return fullbackup, nil
		}
	}

	return fullbackup, errors.New("Not found valid full backup set")
}

func FindSpecifyFullbackupByDB(mCtx *ManagerCtx, dbConf *CommonDBConf, instanceid string, backupid string) (BackupMetaInfo, error) {
	var fullbackup BackupMetaInfo
	var dbInfo *DBInfo
	var err error

	if dbConf.DBType == "pgsql" {
		dbInfo, err = initPgsqlDBInfo(dbConf)
		if err != nil {
			mCtx.logger.Printf("[ERROR] manager initDBInfo failed: %s", err.Error())
			return fullbackup, err
		}

	} else if dbConf.DBType == "mysql" {
		dbInfo, err = initMysqlDBInfo(dbConf)
		if err != nil {
			mCtx.logger.Printf("[ERROR] manager initDBInfo failed: %s", err.Error())
			return fullbackup, err
		}
	} else {
		return fullbackup, errors.New("Not support db type")
	}

	defer dbInfo.db.Close()

	sql := fmt.Sprintf("select instanceid,backupid,backupjobid,file,starttime,endtime,location,status from backup_meta where backuptype = 'full' and instanceid = '%s' and backupid = '%s';", instanceid, backupid)
	mCtx.logger.Printf("[INFO] manager query sql: %s", sql)
	rows, err := queryDB(dbInfo, sql)
	if err != nil {
		return fullbackup, err
	}
	defer rows.Close()

	for rows.Next() {
		var instanceid string
		var backupid string
		var backupjobid string
		var file string
		var starttime int64
		var endtime int64
		var location string
		var status string

		err = rows.Scan(&instanceid, &backupid, &backupjobid, &file, &starttime, &endtime, &location, &status)
		if err != nil {
			return fullbackup, err
		}

		fullbackup.File = file
		fullbackup.StartTime = starttime
		fullbackup.BackupJobID = backupjobid
		fullbackup.EndTime = endtime
		fullbackup.Location = location
		fullbackup.InstanceID = instanceid
		fullbackup.BackupID = backupid
		fullbackup.Status = status
		return fullbackup, nil
	}

	return fullbackup, errors.New("Not found valid full backup set")
}

func GetFullBackupSets(mCtx *ManagerCtx, storage *BackupStorageSpace, funcs *sync.Map, instanceid string) ([]BackupMetaInfo, error) {
	var backups []BackupMetaInfo

	var err error

	var metaFile fileSystem
	metaFile.name = storage.StorageType

	plugin := metaFile.name
	mapping, err := loadFunction("ExportMapping", plugin, funcs)
	if err != nil {
		return backups, err
	}
	entry, err := mapping(nil, nil)
	metaFile.funcs, _ = entry.(map[string]func(interface{}, map[string]interface{}) (interface{}, error))

	pluginInitParam := make(map[string]interface{})
	confMap := make(map[string]interface{})
	confMap["InstanceID"] = instanceid
	confMap["BackupID"] = "fullmetas"

	storageToMap(storage, &confMap)

	content, err := json.MarshalIndent(confMap, "", "\t")
	if err != nil {
		mCtx.logger.Printf("[ERROR] %s make content, failed: %s\n", metaFile.name, err)
		return backups, err
	}
	pluginInitParam["conf"] = content
	metaFile.handle, err = metaFile.funcs["init"](nil, pluginInitParam)
	if err != nil {
		mCtx.logger.Printf("[ERROR] manager init meta filesystem [%s] failed: %s\n", metaFile.name, err.Error())
		return backups, err
	}

	defer func() {
		metaFile.funcs["fini"](metaFile.handle, nil)
	}()

	fullmetaFile, err := OpenFile(metaFile, "fullbackups.pg_o_backup_meta")
	if err != nil {
		mCtx.logger.Printf("[ERROR] open meta file failed. err[%s]\n", err.Error())
		return backups, err
	}

	metaReader, ok := fullmetaFile.(io.Reader)
	if !ok {
		err = errors.New("invalid metaFile")
		return backups, err
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
	Close(metaFile, fullmetaFile)

	i := 0
	for ; i < len(wbuf); i++ {
		if wbuf[i] == '\n' {
			break
		}
	}
	n, err := strconv.Atoi(string(wbuf[0:i]))
	if err != nil {
		mCtx.logger.Printf("[ERROR] convert first line of fullbackups.pg_o_backup_meta failed, err[%s]\n", err.Error())
		return backups, err
	}

	count := 0
	var b, e int
	b = i + 1
	i++
	var starttime, endtime int64
	var backupid, location string
	var useBlock bool

	for ; i < len(wbuf); i++ {
		if wbuf[i] == '\n' {
			e = i
			record := string(wbuf[b:e])
			splits := strings.Split(record, ",")
			/*
			 * backward compatibility: add 4th column that represent type for block backup, no 4th column in previous version
			 */
			if len(splits) < 3 || len(splits) > 4 {
				errstr := "[ERROR] invalid record in full meta: " + record
				err := errors.New(errstr)
				return backups, err
			}

			backupid = splits[0]

			_starttime, err := strconv.Atoi(splits[1])
			if err != nil {
				mCtx.logger.Printf("[ERROR] convert record of fullbackups.pg_o_backup_meta failed, err[%s]\n", err.Error())
				return backups, err
			}
			starttime = int64(_starttime)

			_endtime, err := strconv.Atoi(splits[2])
			if err != nil {
				mCtx.logger.Printf("[ERROR] convert record of fullbackups.pg_o_backup_meta failed, err[%s]\n", err.Error())
				return backups, err
			}
			endtime = int64(_endtime)

			useBlock = false
			if len(splits) == 4 && splits[3] == "block" {
				useBlock = true
			}

			location = storageToLocation(storage, instanceid, backupid)

			backups = append(backups, BackupMetaInfo{
				InstanceID: instanceid,
				BackupID:   backupid,
				StartTime:  starttime,
				EndTime:    endtime,
				Location:   location,
				IsBlock:    useBlock,
			})

			b = i + 1

			count++
			if count == n {
				break
			}
		}
	}

	return backups, nil
}

func CalculateLogicFullbackupByFile(mCtx *ManagerCtx, storage *BackupStorageSpace, funcs *sync.Map, instanceid string, backupid string) (BackupMetaInfo, []BackupMetaInfo, error) {
	var fullbackup BackupMetaInfo
	var blockbackups []BackupMetaInfo

	fullbackups, err := GetFullBackupSets(mCtx, storage, funcs, instanceid)
	if err != nil {
		return fullbackup, blockbackups, errors.New("get full backups from meta failed")
	}

	isFoundFull := false
	isFoundBlock := false
	var lastblockbackup BackupMetaInfo
	for _, fb := range fullbackups {
		if fb.BackupID == backupid {
			if !fb.IsBlock {
				fullbackup = fb
				isFoundFull = true
			} else {
				lastblockbackup = fb
				isFoundBlock = true
			}
			break
		}
	}

	if isFoundFull {
		return fullbackup, blockbackups, nil
	}

	if !isFoundBlock {
		return fullbackup, blockbackups, errors.New("Not found valid full backup set")
	}

	var lastestFullbackupTime int64
	lastestFullbackupTime = 0
	for _, fb := range fullbackups {
		if !fb.IsBlock && fb.StartTime < lastblockbackup.StartTime && fb.StartTime > lastestFullbackupTime {
			fullbackup = fb
			lastestFullbackupTime = fb.StartTime
		}
	}

	if lastestFullbackupTime == 0 {
		return fullbackup, blockbackups, errors.New("Not found valid full backup set before specify block backup")
	}

	for _, fb := range fullbackups {
		if fb.IsBlock && fb.StartTime > fullbackup.StartTime && fb.StartTime <= lastblockbackup.StartTime {
			blockbackups = append(blockbackups, fb)
		}
	}

	return fullbackup, blockbackups, nil
}

func CalculateLogicFullbackupByDB(mCtx *ManagerCtx, storage *BackupStorageSpace, dbConf *CommonDBConf, instanceid string, backupid string) (BackupMetaInfo, []BackupMetaInfo, error) {
	var fullbackup BackupMetaInfo
	var blockbackups []BackupMetaInfo

	location := storageToLocation(storage, instanceid, backupid)

	fullbackup = BackupMetaInfo{
		InstanceID: instanceid,
		BackupID:   backupid,
		Location:   location,
	}
	
	// TODO support block backup

	return fullbackup, blockbackups, nil
}

func CalculateFullbackupByDB(mCtx *ManagerCtx, dbConf *CommonDBConf, recoveryTime int64, instanceid string) (BackupMetaInfo, error) {
	var fullbackup BackupMetaInfo
	var dbInfo *DBInfo
	var err error

	if dbConf.DBType == "pgsql" {
		dbInfo, err = initPgsqlDBInfo(dbConf)
		if err != nil {
			mCtx.logger.Printf("[ERROR] manager initDBInfo failed: %s", err.Error())
			return fullbackup, err
		}

	} else if dbConf.DBType == "mysql" {
		dbInfo, err = initMysqlDBInfo(dbConf)
		if err != nil {
			mCtx.logger.Printf("[ERROR] manager initDBInfo failed: %s", err.Error())
			return fullbackup, err
		}
	} else {
		return fullbackup, errors.New("Not support db type")
	}

	defer dbInfo.db.Close()

	sql := fmt.Sprintf("select instanceid,backupid,backupjobid,file,starttime,endtime,location,status from backup_meta where backuptype = 'full' and instanceid = '%s' and endtime < %d order by endtime DESC limit 1;", instanceid, recoveryTime)
	mCtx.logger.Printf("[INFO] manager query sql: %s", sql)
	rows, err := queryDB(dbInfo, sql)
	if err != nil {
		return fullbackup, err
	}
	defer rows.Close()

	for rows.Next() {
		var instanceid string
		var backupid string
		var backupjobid string
		var file string
		var starttime int64
		var endtime int64
		var location string
		var status string

		err = rows.Scan(&instanceid, &backupid, &backupjobid, &file, &starttime, &endtime, &location, &status)
		if err != nil {
			return fullbackup, err
		}

		if recoveryTime >= endtime {
			fullbackup.File = file
			fullbackup.StartTime = starttime
			fullbackup.BackupJobID = backupjobid
			fullbackup.EndTime = endtime
			fullbackup.Location = location
			fullbackup.InstanceID = instanceid
			fullbackup.BackupID = backupid
			fullbackup.Status = status
		}
		return fullbackup, nil
	}

	return fullbackup, errors.New("Not found valid full backup set")
}

func CalculateBlockbackupsByDB(mCtx *ManagerCtx, dbConf *CommonDBConf, prevEndTime int64, recoveryTime int64, instanceid string) ([]BackupMetaInfo, error) {
	var blockbackups []BackupMetaInfo
	var dbInfo *DBInfo
	var err error

	if dbConf.DBType == "pgsql" {
		dbInfo, err = initPgsqlDBInfo(dbConf)
		if err != nil {
			mCtx.logger.Printf("[ERROR] manager initDBInfo failed: %s", err.Error())
			return blockbackups, err
		}

	} else if dbConf.DBType == "mysql" {
		dbInfo, err = initMysqlDBInfo(dbConf)
		if err != nil {
			mCtx.logger.Printf("[ERROR] manager initDBInfo failed: %s", err.Error())
			return blockbackups, err
		}
	} else {
		return blockbackups, errors.New("Not support db type")
	}

	defer dbInfo.db.Close()

	sql := fmt.Sprintf("select instanceid,backupid,backupjobid,file,starttime,endtime,location,status from backup_meta where backuptype = 'block' and instanceid = '%s' and starttime > %d and endtime < %d order by starttime;", instanceid, prevEndTime, recoveryTime)
	mCtx.logger.Printf("[INFO] manager query sql: %s", sql)
	rows, err := queryDB(dbInfo, sql)
	if err != nil {
		return blockbackups, err
	}
	defer rows.Close()

	for rows.Next() {
		var fullbackup BackupMetaInfo

		var instanceid string
		var backupid string
		var backupjobid string
		var file string
		var starttime int64
		var endtime int64
		var location string
		var status string

		err = rows.Scan(&instanceid, &backupid, &backupjobid, &file, &starttime, &endtime, &location, &status)
		if err != nil {
			return blockbackups, err
		}

		if recoveryTime >= endtime {
			fullbackup.File = file
			fullbackup.StartTime = starttime
			fullbackup.BackupJobID = backupjobid
			fullbackup.EndTime = endtime
			fullbackup.Location = location
			fullbackup.InstanceID = instanceid
			fullbackup.BackupID = backupid
			fullbackup.Status = status
		}
		blockbackups = append(blockbackups, fullbackup)
	}

	return blockbackups, nil
}

func queryBackupRecord(dbInfo *DBInfo, sql string) ([]WalLogMetaInfo, error) {
	rows, err := queryDB(dbInfo, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var increbackups []WalLogMetaInfo
	for rows.Next() {
		var instanceid string
		var backupid string
		var backupjobid string
		var file string
		var starttime int64
		var endtime int64
		var location string
		var status string

		err = rows.Scan(&instanceid, &backupid, &backupjobid, &file, &starttime, &endtime, &location, &status)
		if err != nil {
			return nil, err
		}

		var increbackup WalLogMetaInfo
		increbackup.File = file
		increbackup.StartTime = starttime
		increbackup.EndTime = endtime
		increbackup.Location = location
		increbackup.InstanceID = instanceid
		increbackup.BackupID = backupid
		increbackup.Status = status

		increbackups = append(increbackups, increbackup)
	}
	return increbackups, nil
}

func CalculateIncrebackupsByFile(mCtx *ManagerCtx, storage *BackupStorageSpace, funcs *sync.Map, startTime int64, recoveryTime int64, instanceid string, backupid string) ([]WalLogMetaInfo, error) {
	var increbackups []WalLogMetaInfo
	var err error

	var metaFile fileSystem
	metaFile.name = storage.StorageType

	plugin := metaFile.name
	mapping, err := loadFunction("ExportMapping", plugin, funcs)
	if err != nil {
		return increbackups, err
	}
	entry, err := mapping(nil, nil)
	metaFile.funcs, _ = entry.(map[string]func(interface{}, map[string]interface{}) (interface{}, error))

	pluginInitParam := make(map[string]interface{})
	confMap := make(map[string]interface{})
	confMap["InstanceID"] = instanceid
	if backupid == "" {
		backupid = "increbk"
	}
	confMap["BackupID"] = backupid

	storageToMap(storage, &confMap)

	content, err := json.MarshalIndent(confMap, "", "\t")
	if err != nil {
		mCtx.logger.Printf("[ERROR] %s make content, failed: %s\n", metaFile.name, err)
		return increbackups, err
	}
	pluginInitParam["conf"] = content
	metaFile.handle, err = metaFile.funcs["init"](nil, pluginInitParam)
	if err != nil {
		mCtx.logger.Printf("[ERROR] manager init meta filesystem [%s] failed: %s\n", metaFile.name, err.Error())
		return increbackups, err
	}

	defer func() {
		metaFile.funcs["fini"](metaFile.handle, nil)
	}()

	var ismetaExist, istmpExist, isreadyExist, iserrExist bool

	ret, err := ExistFile(metaFile, "wals.pg_o_backup_meta.err")
	if err != nil {
		mCtx.logger.Printf("[ERROR] exist %s failed: %s\n", "wals.pg_o_backup_meta.err", err.Error())
		return increbackups, err
	}
	iserrExist = ret.(bool)

	ret, err = ExistFile(metaFile, "wals.pg_o_backup_meta")
	if err != nil {
		mCtx.logger.Printf("[ERROR] exist %s failed: %s\n", "wals.pg_o_backup_meta", err.Error())
		return increbackups, err
	}
	ismetaExist = ret.(bool)

	ret, err = ExistFile(metaFile, "wals.pg_o_backup_meta.tmp")
	if err != nil {
		mCtx.logger.Printf("[ERROR] exist %s failed: %s\n", "wals.pg_o_backup_meta", err.Error())
		return increbackups, err
	}
	istmpExist = ret.(bool)

	ret, err = ExistFile(metaFile, "wals.pg_o_backup_meta.tmp.ready")
	if err != nil {
		mCtx.logger.Printf("[ERROR] exist %s failed: %s\n", "wals.pg_o_backup_meta", err.Error())
		return increbackups, err
	}
	isreadyExist = ret.(bool)

	var metafilename string
	if iserrExist {
		mCtx.logger.Printf("[ERROR] can not get wals.pg_o_backup_meta since some err occur when create it\n")
		return increbackups, errors.New("can not get wals.pg_o_backup_meta since some err occur when create it")
	}

	if !istmpExist && !ismetaExist {
		/*
		 * do not return error since it may be not start wals backup yet
		 */
		mCtx.logger.Printf("[WARNING] can not get wals.pg_o_backup_meta or its tmp file\n")
		return increbackups, nil
	}

	if ismetaExist && !istmpExist {
		metafilename = "wals.pg_o_backup_meta"
	} else if istmpExist && !ismetaExist {
		metafilename = "wals.pg_o_backup_meta.tmp"
	} else if isreadyExist {
		metafilename = "wals.pg_o_backup_meta.tmp"
	} else {
		metafilename = "wals.pg_o_backup_meta"
	}

	walmetaFile, err := OpenFile(metaFile, metafilename)
	if err != nil {
		mCtx.logger.Printf("[ERROR] open meta file failed. err[%s]\n", err.Error())
		return increbackups, err
	}

	metaReader, ok := walmetaFile.(io.Reader)
	if !ok {
		err = errors.New("invalid metaFile")
		return increbackups, err
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
	Close(metaFile, walmetaFile)

	var part string
	if len(wbuf) > 200 {
		part = string(wbuf[0:200])
	} else {
		part = string(wbuf)
	}
	mCtx.logger.Printf("[DEBUG] part of origin meta: %s\n", part)

	i := 0
	for ; i < len(wbuf); i++ {
		if wbuf[i] == '\n' {
			break
		}
	}
	n, err := strconv.Atoi(string(wbuf[0:i]))
	if err != nil {
		mCtx.logger.Printf("[ERROR] convert first line of fullbackups.pg_o_backup_meta failed, err[%s]\n", err.Error())
		return increbackups, err
	}

	location := storageToLocation(storage, instanceid, "increbk")

	count := 0
	var b, e int
	b = i + 1
	i++
	var lwals []WalLogMetaInfo
	var rwal WalLogMetaInfo
	var maxStarttime, minStarttime int64
	maxStarttime = 0
	const INT_MAX = int64(^uint(0) >> 1)
	minStarttime = INT_MAX
	timelineExample := "00000002"
	timelineLen := len(timelineExample)
	set := make(map[string]struct{})
	for ; i < len(wbuf); i++ {
		if wbuf[i] == '\n' {
			e = i
			record := string(wbuf[b:e])
			splits := strings.Split(record, ",")
			/*
			 * backward compatibility: add 4th column that represent time mode for wal time, no 4/5th column in previous version
			 */
			if len(splits) < 3 || len(splits) > 5 {
				errstr := "[ERROR] invalid record in wal's meta: " + record
				err := errors.New(errstr)
				return increbackups, err
			}

			walname := splits[0]
			isProcessed := false
			if _, ok := set[walname]; ok {
				isProcessed = true
			}
			set[walname] = struct{}{}

			timeMode := USINGWALTIME
			if len(splits) >= 4 {
				timeMode = splits[3]
			}

			var walsize int64
			if len(splits) >= 5 {
				_walsize, err := strconv.Atoi(splits[4])
				if err != nil {
					return increbackups, err
				}
				walsize = int64(_walsize)
			}

			/*
			 * handle history file and wal file without waltime later
			 */
			if path.Ext(walname) != ".history" && !isProcessed && timeMode == USINGWALTIME {
				_starttime, err := strconv.Atoi(splits[1])
				if err != nil {
					mCtx.logger.Printf("[ERROR] convert record of wals.pg_o_backup_meta failed, err[%s]\n", err.Error())
					return increbackups, err
				}
				starttime := int64(_starttime)

				_endtime, err := strconv.Atoi(splits[2])
				if err != nil {
					mCtx.logger.Printf("[ERROR] convert record of wals.pg_o_backup_meta failed, err[%s]\n", err.Error())
					return increbackups, err
				}
				endtime := int64(_endtime)

				if starttime > startTime && starttime < recoveryTime {
					var wal WalLogMetaInfo
					wal.File = splits[0]
					wal.FileSize = walsize
					wal.StartTime = starttime
					wal.EndTime = endtime
					wal.Location = location
					wal.InstanceID = instanceid
					wal.BackupID = backupid
					increbackups = append(increbackups, wal)
				} else if starttime < startTime && starttime > maxStarttime {
					lwals = nil

					var lwal WalLogMetaInfo
					maxStarttime = starttime
					lwal.StartTime = starttime
					lwal.EndTime = endtime
					lwal.File = splits[0]
					lwal.FileSize = walsize
					lwal.Location = location
					lwal.InstanceID = instanceid
					lwal.BackupID = backupid

					lwals = append(lwals, lwal)
				} else if starttime == maxStarttime {
					var lwal WalLogMetaInfo
					maxStarttime = starttime
					lwal.StartTime = starttime
					lwal.EndTime = endtime
					lwal.File = splits[0]
					lwal.FileSize = walsize
					lwal.Location = location
					lwal.InstanceID = instanceid
					lwal.BackupID = backupid

					lwals = append(lwals, lwal)
				} else if starttime > startTime && starttime < minStarttime {
					minStarttime = starttime
					rwal.StartTime = starttime
					rwal.EndTime = endtime
					rwal.File = splits[0]
					rwal.FileSize = walsize
				}
			}

			b = i + 1

			count++
			if count == n {
				break
			}
		}
	}

	if maxStarttime > 0 {
		increbackups = append(increbackups, lwals...)
	}

	if minStarttime < INT_MAX {
		rwal.Location = location
		rwal.InstanceID = instanceid
		rwal.BackupID = backupid
		increbackups = append(increbackups, rwal)
	}

	increbackups = appendWalsByDictionaryOrder(increbackups, set, location, instanceid, backupid)

	if missList, err := checkWalsByDictionaryOrder(increbackups); err != nil {
		mCtx.logger.Printf("[ERROR] not consistent is found when check wals: %s\n", missList)
		return increbackups, err
	}

	minTimeline := ""
	for _, wal := range increbackups {
		timeline := wal.File[:timelineLen]
		if minTimeline == "" {
			minTimeline = timeline
		} else if timeline <  minTimeline {
			minTimeline = timeline
		}
	}

	var historys []WalLogMetaInfo
	timelineSet := make(map[string]struct{})
	for _, wal := range increbackups {
		timeline := wal.File[:timelineLen]
		/**
		 * min timeline is no need, therefore we exclude it as it may be deleted
		 */
		if timeline == minTimeline {
			continue
		}

		if _, ok := timelineSet[timeline]; ok {
			continue
		}

		if _, ok := set[timeline+".history"]; ok {
			history := wal
			history.File = timeline + ".history"
			history.StartTime = 0
			history.EndTime = 0
			historys = append(historys, history)
		}

		timelineSet[timeline] = struct{}{}
	}
	increbackups = append(increbackups, historys...)

	return increbackups, nil
}

/**
 * some wals may be missed since usingreal time. therefore, we include it by dictionary order
 */
func appendWalsByDictionaryOrder(wals []WalLogMetaInfo, totalWalSet map[string]struct{}, location string, instanceid string, backupid string) []WalLogMetaInfo {
	if len(wals) <= 1 {
		return wals
	}

	minWal := "FFFFFFFFFFFFFFFFFFFFFFFF"
	maxWal := "000000000000000000000000"

	var appends []WalLogMetaInfo
	oriWalSet := make(map[string]struct{})
	for _, wal := range wals {
		if wal.File < minWal {
			minWal = wal.File
		}

		if wal.File > maxWal {
			maxWal = wal.File
		}

		oriWalSet[wal.File] = struct{}{}
	}

	for walName := range totalWalSet {
		if walName > minWal && walName < maxWal && path.Ext(walName) != ".history" {
			if _, ok := oriWalSet[walName]; !ok {
				var wal WalLogMetaInfo
				wal.File = walName
				// wal.StartTime = starttime
				// wal.EndTime = endtime
				wal.Location = location
				wal.InstanceID = instanceid
				wal.BackupID = backupid
				appends = append(appends, wal)
			}
		}
	}

	return append(wals, appends...)
}

type walsSorter []WalLogMetaInfo

func (s walsSorter) Len() int {
    return len(s)
}

func (s walsSorter) Swap(i, j int) {
    s[i], s[j] = s[j], s[i]
}

func (s walsSorter) Less(i, j int) bool {
    return s[i].File < s[j].File
}

/**
 * some wals may be missed, check it
 */
func checkWalsByDictionaryOrder(wals []WalLogMetaInfo) ([]string, error) {
	var missList []string

	if len(wals) <= 1 {
		return missList, nil
	}

	sort.Sort(walsSorter(wals))

	timelineExample := "00000002"
	timelineLen := len(timelineExample)

	/** ignore if no wal size or wal size is diffirent */
	walSize := wals[0].FileSize
	for _, wal := range wals {
		if wal.FileSize != walSize {
			return missList, nil
		}
	}
	if walSize == 0 {
		return missList, nil
	}

	preTimeline := ""
	preWal := ""
	for _, wal := range wals {
		timeline := wal.File[:timelineLen]
		if timeline != preTimeline {
			preTimeline = timeline
			preWal = wal.File
		} else {
			if !checkTwoWalConsistencyByDictionaryOrder(preWal, wal.File, int(walSize)) {
				missList = append(missList, preWal + "-" + wal.File)
			}
			preWal = wal.File
		}
	}

	if len(missList) > 0 {
		return missList, errors.New("not consistent since some wals missed, please check the wals's meta")
	}

	return missList, nil
}

func checkTwoWalConsistencyByDictionaryOrder(preWal string, curWal string, walSize int) bool {
	sysOfNum := 4096 / walSize

	preWalPart0 := preWal[0:8]
    preWalPart1 := preWal[8:16]
    preWalPart2 := preWal[16:24]

    _preWalPart0Int := new(big.Int)
    _preWalPart0Int.SetString(preWalPart0, 16)
    preWalPart0Int := _preWalPart0Int.Int64()

    _preWalPart1Int := new(big.Int)
    _preWalPart1Int.SetString(preWalPart1, 16)
    preWalPart1Int := _preWalPart1Int.Int64()

    _preWalPart2Int := new(big.Int)
    _preWalPart2Int.SetString(preWalPart2, 16)
    preWalPart2Int := _preWalPart2Int.Int64()

    if preWalPart2Int + 1 == int64(sysOfNum) {
        preWalPart1Int = preWalPart1Int + 1
        preWalPart2Int = 0
    } else {
        preWalPart2Int = preWalPart2Int + 1
    }

    preWalPart0 = fmt.Sprintf("%08X", preWalPart0Int)
    preWalPart1 = fmt.Sprintf("%08X", preWalPart1Int)
    preWalPart2 = fmt.Sprintf("%08X", preWalPart2Int)

    curWalExpect := preWalPart0 + preWalPart1 + preWalPart2

    return curWalExpect == curWal
}

func CalculateIncrebackupsByDB(mCtx *ManagerCtx, dbConf *CommonDBConf, startTime int64, recoveryTime int64, instanceid string) ([]WalLogMetaInfo, error) {
	var increbackups []WalLogMetaInfo
	var historys []WalLogMetaInfo
	var dbInfo *DBInfo
	var err error
	if dbConf.DBType == "pgsql" {
		dbInfo, err = initPgsqlDBInfo(dbConf)
	} else if dbConf.DBType == "mysql" {
		dbInfo, err = initMysqlDBInfo(dbConf)
	} else {
		err = errors.New("Not support db type")
	}
	if err != nil {
		mCtx.logger.Printf("[ERROR] manager initDBInfo failed: %s", err.Error())
		return nil, err
	}

	defer dbInfo.db.Close()

	sql := fmt.Sprintf("select instanceid,backupid,backupjobid,file,starttime,endtime,location,status from backup_meta where backuptype = 'wal' and instanceid = '%s' and starttime <= %d order by file DESC, starttime DESC limit 1;", instanceid, startTime)
	start, err := queryBackupRecord(dbInfo, sql)
	if err != nil {
		return nil, err
	}

	if len(start) == 0 {
		mCtx.logger.Printf("[INFO] Not found wal that cover the start time of full backup, sql: %s", sql)
		return increbackups, nil
	}

	/*
	 * we obtain all the wal with the same starttime, since starttime of wals of multi timeline will be same and we do not kwown which timeline to use yet.
	 * .partial files may be recovery too, although it go useless and just for debug.
	 */
	startWalTime := start[0].StartTime
	sql = fmt.Sprintf("select instanceid,backupid,backupjobid,file,starttime,endtime,location,status from backup_meta where backuptype = 'wal' and instanceid = '%s' and starttime = %d;", instanceid, startWalTime)
	start, err = queryBackupRecord(dbInfo, sql)
	if err != nil {
		return nil, err
	}

	sql = fmt.Sprintf("select instanceid,backupid,backupjobid,file,starttime,endtime,location,status from backup_meta where backuptype = 'wal' and instanceid = '%s' and starttime > %d order by starttime ASC limit 1;", instanceid, recoveryTime)
	end, err := queryBackupRecord(dbInfo, sql)
	if err != nil {
		return nil, err
	}

	sql = fmt.Sprintf("select instanceid,backupid,backupjobid,file,starttime,endtime,location,status from backup_meta where backuptype = 'wal' and instanceid = '%s' and starttime > %d and starttime < %d order by starttime;", instanceid, startTime, recoveryTime)
	other, err := queryBackupRecord(dbInfo, sql)
	if err != nil {
		return nil, err
	}

	increbackups = append(increbackups, start...)
	increbackups = append(increbackups, other...)
	increbackups = append(increbackups, end...)

	timelineExample := "00000002"
	timelineLen := len(timelineExample)

	var tmpWals []WalLogMetaInfo
	minTimeline := ""
	for _, wal := range increbackups {
		timeline := wal.File[:timelineLen]
		if minTimeline == "" {
			minTimeline = timeline
		} else if timeline <  minTimeline {
			minTimeline = timeline
		}

		if path.Ext(wal.File) == ".history" {
			continue
		}

		tmpWals = append(tmpWals, wal)
	}
	increbackups = tmpWals

	timelineSet := make(map[string]struct{})
	for _, wal := range increbackups {
		timeline := wal.File[:timelineLen]
		/*
		 * min timeline is no need, therefore we exclude it as it may be deleted
		 */
		 if timeline == minTimeline {
			continue
		}

		if _, ok := timelineSet[timeline]; ok {
			continue
		}

		historyfile := timeline + ".history"
		sql = fmt.Sprintf("select instanceid,backupid,backupjobid,file,starttime,endtime,location,status from backup_meta where backuptype = 'wal' and instanceid = '%s' and file = '%s';", instanceid, historyfile)
		history, err := queryBackupRecord(dbInfo, sql)
		if err != nil {
			return nil, err
		}
		historys = append(historys, history...)

		timelineSet[timeline] = struct{}{}
	}
	increbackups = append(increbackups, historys...)

	return increbackups, nil
}

func CalculateBackups(mCtx *ManagerCtx, backupMetaSource string, dbConf *CommonDBConf, storage *BackupStorageSpace, funcs *sync.Map, recoveryTime int64, recoveryMode string, useBlock bool, instanceid string, fullbkid string, increbkid string) (BackupMetaInfo, []BackupMetaInfo, []WalLogMetaInfo, error) {
	var fullbackup BackupMetaInfo
	var blockbackups []BackupMetaInfo
	var increbackups []WalLogMetaInfo
	var err error
	if recoveryMode == "" {
		recoveryMode = "pitr"
	} else if recoveryMode != "full" && recoveryMode != "pitr" {
		return fullbackup, blockbackups, increbackups, errors.New("Not support recovery mode")
	}

	if !(backupMetaSource == "db" || backupMetaSource == "fs") {
		return fullbackup, blockbackups, increbackups, errors.New("Not support backup Meta Source")
	}

	if backupMetaSource == "db" {
		fullbackup, err = CalculateFullbackupByDB(mCtx, dbConf, recoveryTime, instanceid)
		if err != nil {
			return fullbackup, blockbackups, increbackups, err
		}
	} else if backupMetaSource == "fs" {
		fullbackup, err = CalculateFullbackupByFile(mCtx, storage, funcs, recoveryTime, instanceid)
		if err != nil {
			return fullbackup, blockbackups, increbackups, err
		}
	}

	if useBlock {
		if backupMetaSource == "db" {
			blockbackups, err = CalculateBlockbackupsByDB(mCtx, dbConf, fullbackup.EndTime, recoveryTime, instanceid)
			if err != nil {
				return fullbackup, blockbackups, increbackups, err
			}
		} else if backupMetaSource == "fs" {
			blockbackups, err = CalculateBlockbackupsByFile(mCtx, storage, funcs, fullbackup.EndTime, recoveryTime, instanceid)
			if err != nil {
				return fullbackup, blockbackups, increbackups, err
			}
		}
	}

	/*
	 * start time of full backup or final block backup if exist
	 */
	finalStartTime := fullbackup.StartTime
	if len(blockbackups) > 0 {
		finalStartTime = blockbackups[len(blockbackups)-1].StartTime
	}

	if recoveryMode == "pitr" {
		if backupMetaSource == "db" {
			increbackups, err = CalculateIncrebackupsByDB(mCtx, dbConf, finalStartTime, recoveryTime, instanceid)
			if err != nil {
				return fullbackup, blockbackups, increbackups, err
			}
		} else if backupMetaSource == "fs" {
			increbackups, err = CalculateIncrebackupsByFile(mCtx, storage, funcs, finalStartTime, recoveryTime, instanceid, increbkid)
			if err != nil {
				return fullbackup, blockbackups, increbackups, err
			}
		}
	}
	return fullbackup, blockbackups, increbackups, err
}

/*
 * Send staus callback of full recovery at beginning.
 * When full recovery done, send block recovery request.
 * After all block recovery done, send incremetal recovery request.
 * And then send staus callback of incremental recovery until incremental recovery done.
 * Note that, if full recovery is failed, incremetal recovery request won't be sended.
 */
func MultiStageRecoveryCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	mCtx, ok := ctx.(*ManagerCtx)
	if !ok {
		err := errors.New(`{"error": "ctx must be *ManagerCtx", "code": 1002}`)
		return err.Error(), err
	}

	_req, ok := param["req"]
	if !ok {
		err := errors.New(`{"error": "param.req miss", "code": 1002}`)
		return err.Error(), err
	}

	data, ok := _req.([]byte)
	if !ok {
		err := errors.New(`{"error": "param.req must be []byte", "code":1002}`)
		return err.Error(), err
	}

	var rs RunningStatus
	err := json.Unmarshal(data, &rs)
	if err != nil {
		err := fmt.Errorf(`{"error": "invalid restore status request, parse requst failed: %s", "code": 1002}`, err.Error())
		return err.Error(), err
	}

	id := rs.InstanceID + rs.BackupID + rs.BackupJobID
	if rs.Stage == "Failed" && rs.Type == "Full" {
		mCtx.recoveryMap.Delete(id)
	}

	process := 0
	if rs.Stage == "Finished" && rs.Type == "Incremental" {
		process = 100
	}

	reportReq := make(map[string]interface{})

	statusReq := make(map[string]interface{})
	statusReq["InstanceID"] = rs.InstanceID
	statusReq["BackupID"] = rs.BackupID
	statusReq["BackupJobID"] = rs.BackupJobID
	statusReq["BackedSize"] = rs.BackedSize
	statusReq["Process"] = process
	statusReq["Status"] = rs.Stage
	reportReq["code"] = 0
	reportReq["data"] = statusReq
	if rs.Stage == "Failed" {
		reportReq["code"] = 1
		reportReq["error"] = rs.Error
	}

	content, err := json.MarshalIndent(&reportReq, "", "\t")
	if err != nil {
		return nil, err
	}

	mCtx.logger.Printf("[INFO] manager callback status:\n%s\n", string(content))

	r := bytes.NewReader(content)
	var resp *http.Response
	u, err := url.Parse(rs.Callback)
	if err != nil {
		e := fmt.Errorf(`{"error": "%s", "code": 1002}`, err.Error())
		return e.Error(), e
	}

	u.Path = path.Join(u.Path, "UpdateRecoveryStatus")
	mCtx.logger.Printf("[INFO] manager send recovery status [process:%d] to %s\n", process, u.String())
	resp, err = http.Post(u.String(), "application/json", r)
	if err == nil {
		if resp != nil {
			/*
			 * no need to handle response at final stage
			 */
			if rs.Stage != "Failed" && rs.Stage != "Finished" && rs.Stage != "Stop" {
				err = HandleResponse(mCtx, resp, rs)
				if err != nil {
					mCtx.logger.Printf("[ERROR] manager handle response error: %s\n", err.Error())
				}
			}
			resp.Body.Close()
		}
	}

	if rs.Stage != "Finished" || rs.Type == "Incremental" {
		return `{"error": null, "code": 0}`, nil
	}

	// after full restore finish, call the block restore and incremental restore if exist
	_data, ok := mCtx.recoveryMap.Load(id)
	if ok {
		mCtx.recoveryMap.Delete(id)

		req, ok := _data.(RecoveryRequest)
		if !ok {
			mCtx.logger.Printf("[ERROR] manager can not convert RecoveryRequest of id: %s\n", id)
			err := errors.New(`{"error": "cannot convert to RecoveryRequest", "code": 1002}`)
			return err.Error(), err
		}

		if rs.BackupID == req.FullBackup.BackupID {
			if len(req.BlockBackups) > 0 {
				req.Stage = "block"
				req.BlockBackupIndex = 0
			} else {
				req.Stage = "incremental"
			}
		} else {
			i := 0
			for ; i < len(req.BlockBackups); i++ {
				if rs.BackupID == req.BlockBackups[i].BackupID {
					break
				}
			}

			if i == len(req.BlockBackups)-1 {
				req.Stage = "incremental"
			} else if i < len(req.BlockBackups)-1 {
				req.Stage = "block"
				req.BlockBackupIndex = i + 1
			} else {
				mCtx.logger.Printf("[ERROR] invalid backupid of block backup: %s, since not found in recovery request that stored before\n", rs.BackupID)
				err := errors.New(`{"error": "invalid backupid of block backup, since not found in recovery request that stored before", "code": 1002}`)
				return err.Error(), err
			}
		}

		if req.Stage == "incremental" {
			if len(req.IncreBackups) == 0 {

				/*
				 * At this time, no incremental backup set after recovery block backup set, we should return status that process is 100
				 */
				process = 100
				statusReq["Process"] = process
				reportReq["data"] = statusReq
				content, err := json.MarshalIndent(&reportReq, "", "\t")
				if err != nil {
					return nil, err
				}

				mCtx.logger.Printf("[INFO] manager callback status:\n%s\n", string(content))

				r := bytes.NewReader(content)
				mCtx.logger.Printf("[INFO] manager send recovery status [process:%d] to %s\n", process, u.String())
				resp, err = http.Post(u.String(), "application/json", r)
				if err == nil {
					resp.Body.Close()
				}

				return `{"error": null, "code": 0}`, nil
			}
		}

		dev := req.BackupPBD
		nodes := req.BackupMachineList
		data, err = json.Marshal(req)
		if err != nil {
			err := fmt.Errorf(`{"error": "%s", "code": 1002}`, err.Error())
			return err.Error(), err
		}

		plugin := "pgpipeline"
		if req.Stage == "incremental" {
			plugin = "increpipeline"
		}

		err := BroadcastRequest(mCtx, plugin, nodes, "/Recovery?pbd="+dev, data)
		if err != nil {
			return err.Error(), err
		} else {
			return `{"error": null, "code": 0}`, nil
		}
	} else {
		mCtx.logger.Printf("[ERROR] manager can not found id: %s\n", id)
		err := errors.New(`{"error": "cannot find full recovery", "code": 1002}`)
		return err.Error(), err
	}
}

func CalculateRecoveryCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	var fullbackup BackupMetaInfo
	var blockbackups []BackupMetaInfo
	var increbackups []WalLogMetaInfo

	mCtx, ok := ctx.(*ManagerCtx)
	if !ok {
		err := errors.New(`{"error": "ctx must be *ManagerCtx", "code": 1002}`)
		return err.Error(), err
	}

	_req, ok := param["req"]
	if !ok {
		err := errors.New(`{"error": "param.req miss", "code": 1002}`)
		return err.Error(), err
	}
	data, ok := _req.([]byte)
	if !ok {
		err := errors.New(`{"error": "param.req must be []byte", "code": 1002}`)
		return err.Error(), err
	}
	_funcs, ok := param["imports"]
	if !ok {
		err := errors.New(`{"error": "param.imports miss", "code": 1002}`)
		return err.Error(), err
	}
	funcs, ok := _funcs.(*sync.Map)
	if !ok {
		err := errors.New(`{"error": "param.imports must be *sync.Map", "code": 1002}`)
		return err.Error(), err
	}

	var req CalRecoveryRequest
	err := json.Unmarshal(data, &req)
	if err != nil {
		e := fmt.Errorf(`{"error": "invalid recovery request, parse requst failed: %s", "code": 1002}`, err.Error())
		mCtx.logger.Printf("[ERROR] manager parse recovery request failed")
		return e.Error(), e
	}

	if req.RecoveryMode == "Full" {
		fullbackup, err = CalculateFullbackupByFile(mCtx, &req.BackupStorageSpace, funcs, req.RecoveryTime, req.InstanceID)
		if err != nil {
			return err.Error(), err
		}
	} else if req.RecoveryMode == "Incremental" {
		increbackups, err = CalculateIncrebackupsByFile(mCtx, &req.BackupStorageSpace, funcs, req.StartTime, req.RecoveryTime, req.InstanceID, req.BackupID)
		if err != nil {
			return err.Error(), err
		}
	} else if req.RecoveryMode == "Block" {
		blockbackups, err = CalculateBlockbackupsByFile(mCtx, &req.BackupStorageSpace, funcs, req.EndTime, req.RecoveryTime, req.InstanceID)
		if err != nil {
			return err.Error(), err
		}
	} else {
		e := fmt.Errorf(`{"error": "invalid cal recovery request, not support recovery mode: %s", "code": 1002}`, req.RecoveryMode)
		mCtx.logger.Printf("[ERROR] manager parse recovery request failed: %s", e.Error())
		return e.Error(), e
	}

	req.FullBackup = fullbackup
	req.BlockBackups = blockbackups
	req.IncreBackups = increbackups

	data, err = json.Marshal(req)
	if err != nil {
		e := fmt.Errorf(`{"error": "manager can not marshal CalRecoveryRequest: %s", "code": 1002}`, err.Error())
		mCtx.logger.Printf("[ERROR] manager can not marshal CalRecoveryRequest: %s", err.Error())
		return e.Error(), e
	}

	r := bytes.NewReader(data)
	URL := "http://" + req.ManagerAddr + "/manager/SyncRecoveryInfo"

	resp, err := http.Post(URL, "application/json", r)
	if err != nil {
		e := fmt.Errorf(`{"error": "manager post cm manager SyncRecoveryInfo failed: %s", "code": 1002}`, err.Error())
		mCtx.logger.Printf("[ERROR] manager post cm manager SyncRecoveryInfo failed: %s", err.Error())
		return e.Error(), e
	}
	defer resp.Body.Close()
	mCtx.logger.Printf("[INFO] manager request [%s] success\n", URL)

	respContent, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		e := fmt.Errorf(`{"error": "manager read [%s] response failed: %s", "code": 1002}`, err.Error())
		mCtx.logger.Printf("[ERROR] manager read [%s] response failed: %s", err.Error())
		return e.Error(), e
	}
	var respInfo ResponseData
	err = json.Unmarshal(respContent, &respInfo)
	if err != nil {
		e := fmt.Errorf(`{"error": "manager [%s] response parse failed: %s", "code": 1002}`, URL, err.Error())
		mCtx.logger.Printf("[ERROR] manager [%s] response parse failed: %s", URL, err.Error())
		return e.Error(), e
	}
	if respInfo.Code != 0 {
		e := fmt.Errorf(`{"error": "manager [%s] response get error: %s", "code": 1002}`, URL, respInfo.Error)
		mCtx.logger.Printf("[ERROR] manager [%s] response get error: %s", URL, respInfo.Error)
		return e.Error(), e
	}
	return `{"error": null, "code": 0}`, nil
}

func RecordRecoveryCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	mCtx, ok := ctx.(*ManagerCtx)
	if !ok {
		err := errors.New(`{"error": "ctx must be *ManagerCtx", "code": 1002}`)
		return err.Error(), err
	}

	_req, ok := param["req"]
	if !ok {
		err := errors.New(`{"error": "param.req miss", "code": 1002}`)
		return err.Error(), err
	}
	data, ok := _req.([]byte)
	if !ok {
		err := errors.New(`{"error": "param.req must be []byte", "code": 1002}`)
		return err.Error(), err
	}

	var req RecoveryRequest
	err := json.Unmarshal(data, &req)
	if err != nil {
		e := fmt.Errorf(`{"error": "invalid recovery request, parse requst failed: %s", "code": 1002}`, err.Error())
		mCtx.logger.Printf("[ERROR] manager parse recovery request failed")
		return e.Error(), e
	}

	id := req.RecordID
	mCtx.logger.Printf("[INFO] manager store id %s to recovery map for blockbackup or increbackup later\n", id)
	mCtx.recoveryMap.Store(id, req)

	return `{"error": null, "code": 0}`, nil
}

func QueryIncrementalNodeCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	mCtx, ok := ctx.(*ManagerCtx)
	if !ok {
		err := errors.New(`{"error": "ctx must be *ManagerCtx", "code": 1002}`)
		return err.Error(), err
	}

	_req, ok := param["req"]
	if !ok {
		err := errors.New(`{"error": "param.req miss", "code": 1002}`)
		return err.Error(), err
	}
	data, ok := _req.([]byte)
	if !ok {
		err := errors.New(`{"error": "param.req must be []byte", "code": 1002}`)
		return err.Error(), err
	}

	var req WalHelperRequest
	err := json.Unmarshal(data, &req)
	if err != nil {
		e := fmt.Errorf(`{"error": "invalid recovery request, parse requst failed: %s", "code": 1002}`, err.Error())
		mCtx.logger.Printf("[ERROR] manager parse recovery request failed")
		return e.Error(), e
	}

	instanceid := req.InstanceID
	increbkurl := ""
	mCtx.backupjobs.Range(func(k, v interface{}) bool {
		req := v.(BackupRequest)
		if req.BackupType == "Incremental" && req.InstanceID == instanceid {
			increbkurl = req.BackupMachineList[0]
		}
		return true
	})

	if increbkurl == "" {
		return `{"error": "not found incremental ip", "code": 1}`, nil
	}

	u, err := url.Parse(increbkurl)
	if err != nil {
		return `{"error": "parse incremental url failed", "code": 1}`, nil
	}

	host := u.Host

	ret := fmt.Sprintf(`{"error": null, "code": 0, "data": "%s"}`, strings.Split(host, ":")[0])

	return ret, nil
}

func RecoveryCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	mCtx, ok := ctx.(*ManagerCtx)
	if !ok {
		err := errors.New(`{"error": "ctx must be *ManagerCtx", "code": 1002}`)
		return err.Error(), err
	}

	_req, ok := param["req"]
	if !ok {
		err := errors.New(`{"error": "param.req miss", "code": 1002}`)
		return err.Error(), err
	}
	data, ok := _req.([]byte)
	if !ok {
		err := errors.New(`{"error": "param.req must be []byte", "code": 1002}`)
		return err.Error(), err
	}
	_funcs, ok := param["imports"]
	if !ok {
		err := errors.New(`{"error": "param.imports miss", "code": 1002}`)
		return err.Error(), err
	}
	funcs, ok := _funcs.(*sync.Map)
	if !ok {
		err := errors.New(`{"error": "param.imports must be *sync.Map", "code": 1002}`)
		return err.Error(), err
	}

	var req RecoveryRequest
	err := json.Unmarshal(data, &req)
	if err != nil {
		e := fmt.Errorf(`{"error": "invalid recovery request, parse requst failed: %s", "code": 1002}`, err.Error())
		mCtx.logger.Printf("[ERROR] manager parse recovery request failed")
		return e.Error(), e
	}

	mCtx.logger.Printf("[INFO] manager send restore command to nodes: %s\n", strings.Join(req.BackupMachineList, ","))

	req.Stage = "full"
	var dbConf CommonDBConf
	if req.BackupAccount.Endpoint != "" {
		dbConf.Endpoint = strings.Split(req.BackupAccount.Endpoint, ",")[0]
		dbConf.Port = req.BackupAccount.Port
		dbConf.Username = req.BackupAccount.User
		dbConf.Password = req.BackupAccount.Password
		dbConf.Database = req.BackupAccount.Database
		dbConf.DBType = "pgsql"
	}

	if mCtx.metaDBConf.Endpoint != "" && mCtx.metaDBConf.DBType != "" {
		dbConf.Endpoint = strings.Split(mCtx.metaDBConf.Endpoint, ",")[0]
		dbConf.Port = mCtx.metaDBConf.Port
		dbConf.Username = mCtx.metaDBConf.Username
		dbConf.Password = mCtx.metaDBConf.Password
		dbConf.Database = mCtx.metaDBConf.Database
		dbConf.DBType = mCtx.metaDBConf.DBType
	}

	if !(req.BackupMetaSource == "db" || req.BackupMetaSource == "fs") {
		e := fmt.Errorf(`{"error": "not support backup Meta Source, please check the BackupMetaSource param", "code": 1002}`)
		mCtx.logger.Printf("[ERROR] manager calculate recovery backup sets failed, not support backup Meta Source, please check the param: BackupMetaSource")
		return e.Error(), e
	}

	if req.RecoveryTime != 0 {
		var fullBackup BackupMetaInfo
		var blockBackups []BackupMetaInfo
		var increBackups []WalLogMetaInfo

		if req.Full.BackupID != "" && req.Full.InstanceID != "" {
			// specify full backupid, we need to find the specify full backup set and then calculate the wals
			if req.BackupMetaSource == "db" {
				fullBackup, err = FindSpecifyFullbackupByDB(mCtx, &dbConf, req.Full.InstanceID, req.Full.BackupID)
				if err != nil {
					e := fmt.Errorf(`{"error": "can not find specify fullbackup by db, error: %s, please check the backup_meta and request param: Full", "code": 1002}`, err.Error())
					mCtx.logger.Printf("[ERROR] manager find specify fullbackup by db failed, please check the param: Full")
					return e.Error(), e
				}
			} else {
				fullBackup, err = FindSpecifyFullbackupByFile(mCtx, &req.BackupStorageSpace, funcs, req.Full.InstanceID, req.Full.BackupID)
				if err != nil {
					e := fmt.Errorf(`{"error": "can not find specify fullbackup by file, error: %s, please check the backup_meta and request param: Full", "code": 1002}`, err.Error())
					mCtx.logger.Printf("[ERROR] manager find specify fullbackup by file failed, please check the param: Full")
					return e.Error(), e
				}
			}
			// TODO if block backup, find the lastest full backup and other block backups from lastest full backup to current block backup with fs or db mode

			if req.BackupMetaSource == "db" {
				increBackups, err = CalculateIncrebackupsByDB(mCtx, &dbConf, fullBackup.StartTime, req.RecoveryTime, req.Full.InstanceID)
			} else {
				increBackups, err = CalculateIncrebackupsByFile(mCtx, &req.BackupStorageSpace, funcs, fullBackup.StartTime, req.RecoveryTime, req.Full.InstanceID, req.Incremental.BackupID)
			}
		} else {
			fullBackup, blockBackups, increBackups, err = CalculateBackups(mCtx, req.BackupMetaSource, &dbConf, &req.BackupStorageSpace, funcs, req.RecoveryTime, req.RecoveryMode, req.UseBlock, req.Full.InstanceID, req.Full.BackupID, req.Incremental.BackupID)
			if err != nil {
				e := fmt.Errorf(`{"error": "invalid recovery request, manager calculate recovery backup sets failed: %s", "code": 1002}`, err.Error())
				mCtx.logger.Printf("[ERROR] manager calculate recovery backup sets failed")
				return e.Error(), e
			}
		}

		req.FullBackup = fullBackup
		req.BlockBackups = blockBackups
		req.IncreBackups = increBackups
		if len(blockBackups) == 0 && len(increBackups) == 0 {
			req.Stage = "fullonly"
		}
	} else {
		req.FullBackup.BackupID = req.Full.BackupID
		req.FullBackup.InstanceID = req.Full.InstanceID
		req.FullBackup.BackupJobID = req.Full.BackupJobID
		req.Stage = "fullonly"
		req.FullBackup.Location = storageToLocation(&req.BackupStorageSpace, req.Full.InstanceID, req.Full.BackupID)
		// TODO if block backup, find the lastest full backup and other block backups from lastest full backup to current block backup with fs or db mode
		if req.BackupMetaSource == "db" {
			req.FullBackup, req.BlockBackups, err = CalculateLogicFullbackupByDB(mCtx, &req.BackupStorageSpace, &dbConf, req.Full.InstanceID, req.Full.BackupID)
		} else {
			req.FullBackup, req.BlockBackups, err = CalculateLogicFullbackupByFile(mCtx, &req.BackupStorageSpace, funcs, req.Full.InstanceID, req.Full.BackupID)
		}
		if err != nil {
			e := fmt.Errorf(`{"error": "invalid recovery request, manager calculate logic full backup failed: %s", "code": 1002}`, err.Error())
			mCtx.logger.Printf("[ERROR] manager calculate logic full backup failed: %s\n", err.Error())
			return e.Error(), e
		}
	}

	if req.FullBackup.BackupID != "" {
		mCtx.logger.Printf("[INFO] manager will recovery full backup set: %s", req.FullBackup.BackupID)
	}
	if len(req.BlockBackups) > 0 {
		for _, blockbackup := range req.BlockBackups {
			mCtx.logger.Printf("[INFO] manager will recovery block backup set: %s", blockbackup.BackupID)
		}
	}
	if len(req.IncreBackups) > 0 {
		for _, increbackup := range req.IncreBackups {
			mCtx.logger.Printf("[INFO] manager will recovery wal: %s", increbackup.File)
		}
	}

	var recoveryStatus RecoveryStatus
	recoveryStatus.WalsNum = len(req.IncreBackups)
	recoveryStatus.InstanceID = req.InstanceID
	recoveryStatus.BackupID = req.FullBackup.BackupID
	recoveryStatus.BackupJobID = req.FullBackup.BackupJobID
	mCtx.recoveryStatusMap.Store(req.InstanceID, recoveryStatus)

	data, err = json.Marshal(req)
	if err != nil {
		return err.Error(), err
	}

	mCtx.logger.Printf("[INFO] manager send recovery req: \n %s\n", string(data))

	dev := req.BackupPBD
	nodes := req.BackupMachineList
	plugin := "pgpipeline"
	if req.Filesystem == "pfs.chunk" {
		plugin = "pipeline"
	}

	err = BroadcastRequest(mCtx, plugin, nodes, "/Recovery?pbd="+dev, data)
	if err != nil {
		return err.Error(), err
	}

	return `{"error": null, "code": 0}`, nil
}

func StartBackupCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	mCtx, ok := ctx.(*ManagerCtx)
	if !ok {
		err := errors.New(`{"error": "ctx must be *ManagerCtx", "code": 1002}`)
		return err.Error(), err
	}

	_req, ok := param["req"]
	if !ok {
		err := errors.New(`{"error": "param.req miss", "code": 1002}`)
		return err.Error(), err
	}
	data, ok := _req.([]byte)
	if !ok {
		err := errors.New(`{"error": "param.req must be []byte", "code": 1002}`)
		return err.Error(), err
	}

	var req BackupRequest
	err := json.Unmarshal(data, &req)
	if err != nil {
		mCtx.logger.Printf("[ERROR] manager parse backup request failed")
		e := fmt.Errorf(`{"error": "invalid backup request, parse requst failed: %s", "code": 1002}`, err.Error())
		return e.Error(), e
	}

	var pgDBConf CommonDBConf
	if req.BackupAccount.Endpoint != "" {
		pgDBConf.Endpoint = strings.Split(req.BackupAccount.Endpoint, ",")[0]
		pgDBConf.Port = req.BackupAccount.Port
		pgDBConf.Username = req.BackupAccount.User
		pgDBConf.Password = req.BackupAccount.Password
	} else {
		e := errors.New(`{"error": "invalid backup request, req.PgDBConf miss", "code": 1002}`)
		return e.Error(), e
	}

	if req.ManagerAddr == "" {
		ip, err := getHostIp(mCtx)
		if err != nil {
			mCtx.logger.Printf("[ERROR] can not get legal host ip: %s\n", err.Error())
			e := errors.New(`{"error": "invalid backup request, can not get host ip", "code": 1002}`)
			return e.Error(), e
		}

		req.ManagerAddr = ip + ":" + mCtx.serverPort
	}

	id := req.InstanceID + req.BackupID + req.BackupJobID
	mCtx.backupjobs.Store(id, req)
	mCtx.gbpsc.needUpdate = true

	data, err = json.Marshal(req)
	if err != nil {
		e := errors.New(`{"error": "invalid backup request, remarshal req failed", "code": 1002}`)
		return e.Error(), e
	}

	param["req"] = data

	if req.BackupType == "Full" {
		return StartFullBackupCallback(ctx, param)
	} else if req.BackupType == "Incremental" {
		return StartIncreBackupCallback(ctx, param)
	} else {
		err := errors.New(`{"error": "backup mode not support", "code": 1002}`)
		return err.Error(), err
	}
}

func StartIncreBackupCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	mCtx, ok := ctx.(*ManagerCtx)
	if !ok {
		err := errors.New(`{"error": "ctx must be *ManagerCtx", "code": 1002}`)
		return err.Error(), err
	}

	_req, ok := param["req"]
	if !ok {
		err := errors.New(`{"error": "param.req miss", "code": 1002}`)
		return err.Error(), err
	}
	data, ok := _req.([]byte)
	if !ok {
		err := errors.New(`{"error": "param.req must be []byte", "code": 1002}`)
		return err.Error(), err
	}

	var req BackupRequest
	err := json.Unmarshal(data, &req)
	if err != nil {
		mCtx.logger.Printf("[ERROR] manager parse backup request failed")
		e := fmt.Errorf(`{"error": "invalid backup request, parse requst failed: %s", "code": 1002}`, err.Error())
		return e.Error(), e
	}

	mCtx.logger.Printf("[INFO] manager send backup command to nodes: %s\n", strings.Join(req.BackupMachineList, ","))

	nodes := req.BackupMachineList
	dev := req.BackupPBD

	if mCtx.metaDBConf.DBType != "" {
		req.MetaAccount.Endpoint = mCtx.metaDBConf.Endpoint
		req.MetaAccount.Port = mCtx.metaDBConf.Port
		req.MetaAccount.User = mCtx.metaDBConf.Username
		req.MetaAccount.Password = mCtx.metaDBConf.Password
		req.MetaAccount.Database = mCtx.metaDBConf.Database
		req.MetaAccount.DBType = mCtx.metaDBConf.DBType
	}

	data, err = json.Marshal(req)
	if err != nil {
		return err.Error(), err
	}

	// TODO increbackup should be only run in one machine such as manager endpoint
	err = BroadcastRequest(mCtx, "increpipeline", nodes, "/StartIncreBackup?pbd="+dev, data)
	if err != nil {
		return err.Error(), err
	} else {
		return `{"error": null, "code": 0}`, nil
	}
}

func StartFullBackupCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	mCtx, ok := ctx.(*ManagerCtx)
	if !ok {
		err := errors.New(`{"error": "ctx must be *ManagerCtx", "code": 1002}`)
		return err.Error(), err
	}

	_req, ok := param["req"]
	if !ok {
		err := errors.New(`{"error": "param.req miss", "code": 1002}`)
		return err.Error(), err
	}
	data, ok := _req.([]byte)
	if !ok {
		err := errors.New(`{"error": "param.req must be []byte", "code": 1002}`)
		return err.Error(), err
	}

	var req BackupRequest
	err := json.Unmarshal(data, &req)
	if err != nil {
		mCtx.logger.Printf("[ERROR] manager parse backup request failed")
		e := fmt.Errorf(`{"error": "invalid backup request, parse requst failed: %s", "code": 1002}`, err.Error())
		return e.Error(), e
	}

	mCtx.logger.Printf("[INFO] manager send backup command to nodes: %s\n", strings.Join(req.BackupMachineList, ","))

	var dev string
	if req.BackupPBD != "" {
		dev = req.BackupPBD
	}
	if mCtx.mode == "chunk" {
		if mCtx.snapshot == "" {
			err := errors.New("snapshot config in manager.conf should not be null in chunk mode")
			e := fmt.Errorf(`{"error": "invalid backup request, parse requst failed: %s", "code": 1002}`, err.Error())
			return e.Error(), e
		}

		dbInfo, err := initPgsqlDBInfo(&req.PgDBConf)
		if err != nil {
			mCtx.logger.Printf("[ERROR] manager initDBInfo failed: %s", err.Error())
			return err.Error(), err
		}
		mCtx.logger.Printf("[INFO] manager initDBInfo\n")

		dbInfo.execPGStopBK = false
		defer closePGConnect(dbInfo)

		err = pgStartBackup(dbInfo)
		if err != nil {
			mCtx.logger.Printf("[ERROR] manager pgStartBackup failed: %s", err.Error())
			return err.Error(), err
		}
		mCtx.logger.Printf("[INFO] manager pgStartBackup\n")

		hostlist, err := getHostList(req.BackupMachineList)
		if err != nil {
			mCtx.logger.Printf("[ERROR] manager getHostList failed: %s", err.Error())
			return err.Error(), err
		}

		err = exeBashWithParams("createsnapshop.sh", mCtx.snapshot, dev, hostlist)
		if err != nil {
			mCtx.logger.Printf("[ERROR] manager exeBash failed: %s", err.Error())
			return err.Error(), err
		}

		dev = "mapper_" + mCtx.pbd + "-snapshot"

		err = pgStopBackup(dbInfo)
		dbInfo.execPGStopBK = true
		if err != nil {
			mCtx.logger.Printf("[ERROR] manager pgStopBackup failed: %s", err.Error())
			return err.Error(), err
		}
		mCtx.logger.Printf("[INFO] manager pgStopBackup\n")
	}

	if mCtx.metaDBConf.DBType != "" {
		req.MetaAccount.Endpoint = mCtx.metaDBConf.Endpoint
		req.MetaAccount.Port = mCtx.metaDBConf.Port
		req.MetaAccount.User = mCtx.metaDBConf.Username
		req.MetaAccount.Password = mCtx.metaDBConf.Password
		req.MetaAccount.Database = mCtx.metaDBConf.Database
		req.MetaAccount.DBType = mCtx.metaDBConf.DBType
	}

	req.BackupPBD = dev
	data, err = json.Marshal(req)
	if err != nil {
		return err.Error(), err
	}

	nodes := req.BackupMachineList
	plugin := "pgpipeline"
	if req.Filesystem == "pfs.chunk" {
		plugin = "pipeline"
	}
	err = BroadcastRequest(mCtx, plugin, nodes, "/StartFullBackup?pbd="+dev, data)
	if err != nil {
		return err.Error(), err
	} else {
		return `{"error": null, "code": 0}`, nil
	}
}

func LimitBackupCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	mCtx, ok := ctx.(*ManagerCtx)
	if !ok {
		err := errors.New(`{"error": "ctx must be *ManagerCtx", "code": 1002}`)
		return err.Error(), err
	}

	_req, ok := param["req"]
	if !ok {
		err := errors.New(`{"error": "param.req miss", "code": 1002}`)
		return err.Error(), err
	}
	data, ok := _req.([]byte)
	if !ok {
		err := errors.New(`{"error": "param.req must be []byte", "code": 1002}`)
		return err.Error(), err
	}

	var req LimitRequest
	err := json.Unmarshal(data, &req)
	if err != nil {
		mCtx.logger.Printf("[ERROR] manager parse limit request failed")
		e := fmt.Errorf(`{"error": "invalid limit request, parse requst failed: %s", "code": 1002}`, err.Error())
		return e.Error(), e
	}

	// mCtx.gbpsc.MaxBackupSpeed = req.MaxBackupSpeed
	// mCtx.gbpsc.MaxTotalBackupSpeed = req.MaxTotalBackupSpeed
	// mCtx.logger.Printf("[INFO] manager limit backup speed to total:%d MB/s, single:%d MB/s\n", req.MaxTotalBackupSpeed, mCtx.gbpsc.MaxBackupSpeed, )

	// mCtx.gbpsc.needUpdate = true
	// err = LimitBackupCore(mCtx)

	id := req.InstanceID + req.BackupID + req.BackupJobID
	mCtx.logger.Printf("[INFO] manager limit %s backup speed to %d MB/s\n", id, req.MaxBackupSpeed)

	var newReq BackupRequest
	newReq.InstanceID = req.InstanceID
	newReq.BackupID = req.BackupID
	newReq.BackupJobID = req.BackupJobID
	newReq.MaxBackupSpeed = req.MaxBackupSpeed

	nodes := req.BackupMachineList
	plugin := "pgpipeline"

	if job, ok := mCtx.backupjobs.Load(id); ok {
		jobreq := job.(BackupRequest)
		nodes = jobreq.BackupMachineList

		plugin = "increpipeline"
		if jobreq.BackupType == "Full" {
			plugin = "pgpipeline"
			if jobreq.Filesystem == "pfs.chunk" {
				plugin = "pipeline"
			}
		}
	}

	if len(nodes) == 0 {
		e := fmt.Errorf(`{"error": "invalid limit request, please specify BackupMachineList", "code": 1002}`)
		return e.Error(), e
	}

	data, err = json.Marshal(newReq)
	if err != nil {
		mCtx.logger.Printf("[ERROR] manager parse job request failed")
		return err.Error(), err
	}
	err = BroadcastRequest(mCtx, plugin, nodes, "/Limit", data)
	if err != nil {
		mCtx.logger.Printf("[ERROR] manager broadcast job request failed: %s", err.Error())
		return err.Error(), err
	}

	return `{"error": null, "code": 0}`, nil
}

func LimitBackupCore(mCtx *ManagerCtx) error {
	if !mCtx.gbpsc.isRunning {
		mCtx.gbpsc.isRunning = true
		go func() {
			for true {
				if mCtx.gbpsc.needUpdate {
					mCtx.gbpsc.needUpdate = false

					time.Sleep(100 * time.Millisecond) // wait for 100ms to ensure backup is already running
					fullCount := 0
					incrCount := 0
					mCtx.backupjobs.Range(func(k, v interface{}) bool {
						req := v.(BackupRequest)
						if req.BackupType == "Full" {
							fullCount += 1
						} else if req.BackupType == "Incremental" {
							incrCount += 1
						}
						return true
					})

					var fullSpeed, incrSpeed int
					totalSpeed := mCtx.gbpsc.MaxTotalBackupSpeed * 1024 * 1024
					if fullCount > 0 && incrCount > 0 {
						fullSpeed = (totalSpeed * 8 / 10) / fullCount
						incrSpeed = (totalSpeed * 2 / 10) / incrCount
					} else if fullCount > 0 {
						fullSpeed = totalSpeed / fullCount
					} else if incrCount > 0 {
						incrSpeed = totalSpeed / incrCount
					}

					mCtx.backupjobs.Range(func(k, v interface{}) bool {
						req := v.(BackupRequest)
						nodes := req.BackupMachineList
						plugin := "increpipeline"
						req.MaxBackupSpeed = incrSpeed
						if req.BackupType == "Full" {
							plugin = "pgpipeline"
							if req.Filesystem == "pfs.chunk" {
								plugin = "pipeline"
							}
							req.MaxBackupSpeed = fullSpeed
						}
						data, err := json.Marshal(req)
						if err != nil {
							mCtx.logger.Printf("[ERROR] manager parse limit request failed")
							return true
						}
						err = BroadcastRequest(mCtx, plugin, nodes, "/Limit", data)
						if err != nil {
							mCtx.logger.Printf("[ERROR] manager parse limit request failed: %s", err.Error())
							return true
						}
						return true
					})
				}

				time.Sleep(100 * time.Millisecond)
			}
		}()
	}

	return nil
}

//////////////////////////////////////////////////////////////////////////////////////////////////////

func closePGConnect(dbInfo *DBInfo) {
	if !dbInfo.execPGStopBK {
		pgStopBackup(dbInfo)
	}
	dbInfo.db.Close()
}

func getHostList(mlist []string) (string, error) {
	if len(mlist) == 0 {
		return "", errors.New("Backup machine list is null")
	}
	var newmlist []string
	for _, m := range mlist {
		if m[:7] == "http://" {
			newmlist = append(newmlist, m[7:len(m)])
		} else {
			newmlist = append(newmlist, m)
		}
	}

	hostlist := strings.Split(newmlist[0], ":")[0]
	size := len(newmlist)
	for i := 1; i < size; i++ {
		hostlist = hostlist + "," + strings.Split(newmlist[i], ":")[0]
	}
	if len(hostlist) == 0 {
		return hostlist, errors.New("Invalid host list, please check the machine list")
	}
	return hostlist, nil
}

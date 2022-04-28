package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

func StopBackupCallback(plugin string, data []byte, notify chan []byte) (string, error) {
	var req StopBackupRequest
	err := json.Unmarshal(data, &req)
	if err != nil {
		e := fmt.Errorf(`{"error": "invalid stop request, parse requst failed: %s", "code": 1002}`, err.Error())
		return e.Error(), e
	}

	if plugin == "pipeline" {
		if req.Filesystem == "pfs.chunk" {
			plugin = "pipeline"
		} else {
			plugin = "pgpipeline"
		}
	}

	externConf := make(map[string]interface{})
	var job BackupJob
	job.Action = "stop"
	job.BackupAccount = req.BackupAccount
	job.MetaAccount = req.MetaAccount
	job.BackupID = req.BackupID
	job.BackupJobID = req.BackupJobID
	job.InstanceID = req.InstanceID
	job.PGType = req.PGType

	externConf["task"] = job

	msg := &Message{
		Cmd:        "stop",
		Plugin:     plugin,
		InstanceID: req.BackupID + req.InstanceID + req.BackupJobID,
		Data:       externConf,
	}

	err = SendMsg(msg, notify)
	if err != nil {
		e := fmt.Errorf(`{"error": %s, "code": 1}`, err.Error())
		return e.Error(), e
	}
	return `{"error": null, "code": 0}`, nil
}

func UpdateTopologyCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
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

	_notify, ok := param["notify"]
	if !ok {
		err := errors.New(`{"error": "param.notify miss", "code": 1002}`)
		return err.Error(), err
	}
	notify, ok := _notify.(chan []byte)
	if !ok {
		err := errors.New(`{"error": "param.notify must be chan []byte", "code": 1002}`)
		return err.Error(), err
	}

	var req UpdateTopologyRequest
	err := json.Unmarshal(data, &req)
	if err != nil {
		e := fmt.Errorf(`{"error": "invalid update topology request, parse requst failed: %s", "code": 1002}`, err.Error())
		return e.Error(), e
	}

	externConf := make(map[string]interface{})
	var job BackupJob
	job.Action = "updatetopo"
	job.BackupID = req.BackupID
	job.BackupJobID = req.BackupJobID
	job.InstanceID = req.InstanceID
	job.Master = req.Master
	externConf["task"] = job

	plugin := "increpipeline"
	msg := &Message{
		Cmd:        "updatetopo",
		Plugin:     plugin,
		InstanceID: req.BackupID + req.InstanceID + req.BackupJobID,
		Data:       externConf,
	}

	err = SendMsg(msg, notify)
	if err != nil {
		e := fmt.Errorf(`{"error": %s, "code": 1}`, err.Error())
		return e.Error(), e
	}
	return `{"error": null, "code": 0}`, nil
}

func LimitBackupCallback(plugin string, data []byte, notify chan []byte) (string, error) {
	var req BackupRequest
	err := json.Unmarshal(data, &req)
	if err != nil {
		e := fmt.Errorf(`{"error": "invalid stop request, parse requst failed: %s", "code": 1002}`, err.Error())
		return e.Error(), e
	}

	externConf := make(map[string]interface{})
	var job BackupJob
	job.Action = "limit"
	job.BackupID = req.BackupID
	job.BackupJobID = req.BackupJobID
	job.InstanceID = req.InstanceID
	job.MaxBackupSpeed = req.MaxBackupSpeed
	externConf["task"] = job

	msg := &Message{
		Cmd:        "limit",
		Plugin:     plugin,
		InstanceID: req.BackupID + req.InstanceID + req.BackupJobID,
		Data:       externConf,
	}

	err = SendMsg(msg, notify)
	if err != nil {
		e := fmt.Errorf(`{"error": %s, "code": 1}`, err.Error())
		return e.Error(), e
	}
	return `{"error": null, "code": 0}`, nil
}

func StopIncreBackupCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
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

	_notify, ok := param["notify"]
	if !ok {
		err := errors.New(`{"error": "param.notify miss", "code": 1002}`)
		return err.Error(), err
	}
	notify, ok := _notify.(chan []byte)
	if !ok {
		err := errors.New(`{"error": "param.notify must be chan []byte", "code": 1002}`)
		return err.Error(), err
	}

	return StopBackupCallback("increpipeline", data, notify)
}

func LimitIncreBackupCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
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

	_notify, ok := param["notify"]
	if !ok {
		err := errors.New(`{"error": "param.notify miss", "code": 1002}`)
		return err.Error(), err
	}
	notify, ok := _notify.(chan []byte)
	if !ok {
		err := errors.New(`{"error": "param.notify must be chan []byte", "code": 1002}`)
		return err.Error(), err
	}

	return LimitBackupCallback("increpipeline", data, notify)
}

func StopFullBackupCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
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

	_notify, ok := param["notify"]
	if !ok {
		err := errors.New(`{"error": "param.notify miss", "code": 1002}`)
		return err.Error(), err
	}
	notify, ok := _notify.(chan []byte)
	if !ok {
		err := errors.New(`{"error": "param.notify must be chan []byte", "code": 1002}`)
		return err.Error(), err
	}

	return StopBackupCallback("pipeline", data, notify)
}

func LimitFullBackupCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
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

	_notify, ok := param["notify"]
	if !ok {
		err := errors.New(`{"error": "param.notify miss", "code": 1002}`)
		return err.Error(), err
	}
	notify, ok := _notify.(chan []byte)
	if !ok {
		err := errors.New(`{"error": "param.notify must be chan []byte", "code": 1002}`)
		return err.Error(), err
	}

	return LimitBackupCallback("pgpipeline", data, notify)
}

func StopCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
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

	_notify, ok := param["notify"]
	if !ok {
		err := errors.New(`{"error": "param.notify miss", "code": 1002}`)
		return err.Error(), err
	}
	notify, ok := _notify.(chan []byte)
	if !ok {
		err := errors.New(`{"error": "param.notify must be chan []byte", "code": 1002}`)
		return err.Error(), err
	}

	var req StopBackupRequest
	err := json.Unmarshal(data, &req)
	if err != nil {
		e := fmt.Errorf(`{"error": "invalid stop request, parse requst failed: %s", "code": 1002}`, err.Error())
		return e.Error(), e
	}

	msg := &Message{
		Cmd:        "stop",
		Plugin:     "main",
		InstanceID: req.BackupID + req.InstanceID + req.BackupJobID,
	}

	err = SendMsg(msg, notify)
	if err != nil {
		e := fmt.Errorf(`{"error": %s, "code": 1}`, err.Error())
		return e.Error(), e
	}
	return `{"error": null, "code": 0}`, nil
}

func HeartBeatCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	return `{"error": null, "code": 0}`, nil
}

func RecoveryCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
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
		e := fmt.Errorf(`{"error": "invalid backup request, parse requst failed: %s", "code": 1002}`, err.Error())
		return e.Error(), e
	}

	_notify, ok := param["notify"]
	if !ok {
		err := errors.New(`{"error": "param.notify miss", "code": 1002}`)
		return err.Error(), err
	}
	notify, ok := _notify.(chan []byte)
	if !ok {
		err := errors.New(`{"error": "param.notify must be chan []byte", "code": 1002}`)
		return err.Error(), err
	}

	_query, ok := param["query"]
	if !ok {
		err := errors.New(`{"error": "param.query miss", "code": 1002}`)
		return err.Error(), err
	}
	query, ok := _query.(url.Values)
	if !ok {
		err := errors.New(`{"error": "param.notify must be url.Values", "code": 1002}`)
		return err.Error(), err
	}

	pbd := query.Get("pbd")

	externConf := make(map[string]interface{})
	dbsConf := make(map[string]interface{})
	pfsConf := make(map[string]interface{})
	fsbkConf := make(map[string]interface{})
	fs2Conf := make(map[string]interface{})
	s3Conf := make(map[string]interface{})
	httpConf := make(map[string]interface{})
	localfsConf := make(map[string]interface{})

	// common
	externConf["dbs"] = dbsConf
	externConf["pfs"] = pfsConf
	externConf["fsbk"] = fsbkConf
	externConf["fs2"] = fs2Conf
	externConf["s3"] = s3Conf
	externConf["http"] = httpConf
	externConf["localfs"] = localfsConf

	if pbd != "" {
		pfsConf["Pbd"] = pbd
	}
	pfsConf["Flags"] = "restore"

	plugin := "pgpipeline"
	if req.Filesystem == "pfs.chunk" {
		plugin = "pipeline"
	}

	var job BackupJob
	job.BackupID = req.FullBackup.BackupID
	job.InstanceID = req.FullBackup.InstanceID
	job.BackupJobID = req.BackupJobID
	job.WorkerCount = req.WorkerCount
	job.CallbackURL = req.CallbackURL
	job.BackupNodes = req.BackupMachineList
	job.RecoveryFolder = req.RecoveryFolder
	job.DBClusterMetaDir = req.DBClusterMetaDir
	job.PGType = req.PGType
	job.EnableEncryption = req.EnableEncryption
	job.EncryptionPassword = req.EncryptionPassword
	job.Action = "restore"
	//job.Backend = req.BackupStorageSpace.StorageType
	job.Pbd = pbd
	if strings.HasPrefix(req.Filesystem, "pfs.") {
		job.Frontend = "pfs"
	} else {
		job.Frontend = "fs2"
		fs2Conf["Path"] = job.RecoveryFolder
	}

	if req.DBClusterMetaDir != "" {
		localfsConf["Path"] = req.DBClusterMetaDir
	}

	insid := req.FullBackup.BackupID + req.FullBackup.InstanceID + req.BackupJobID
	msg := &Message{
		Cmd:        "restore",
		Plugin:     plugin,
		InstanceID: insid,
		Data:       externConf,
	}

	var location string
	switch req.Stage {
	case "full":
		job.RecoveryType = "full"
		location = req.FullBackup.Location
		break
	case "block":
		job.RecoveryType = "full"
		location = req.BlockBackups[req.BlockBackupIndex].Location
		job.InstanceID = req.BlockBackups[req.BlockBackupIndex].InstanceID
		job.BackupID = req.BlockBackups[req.BlockBackupIndex].BackupID
		job.BackupJobID = req.BlockBackups[req.BlockBackupIndex].BackupJobID
		break
	case "incremental":
		msg.Plugin = "increpipeline"

		for _, increBackup := range req.IncreBackups {
			job.LogFiles = append(job.LogFiles, increBackup.File)
			job.InstanceID = increBackup.InstanceID
			job.BackupID = increBackup.BackupID
			job.BackupJobID = increBackup.BackupJobID
		}

		job.RecoveryType = "incremental"
		location = req.IncreBackups[0].Location
		break
	case "fullonly":
		job.RecoveryType = "fullonly"
		location = req.FullBackup.Location
		break
	default:
		break
	}

	u, err := url.ParseRequestURI(location)
	if err != nil {
		return err.Error(), err
	}

	job.Backend = u.Scheme
	//q := u.Query()
	//srcInsId := q.Get("InstanceID")
	//srcBckId := q.Get("BackupID")

	switch u.Scheme {
	case "file":
		job.Backend = "fsbk"
		path, _ := ParsePath(location)
		fsbkConf["Path"] = path
		fsbkConf["InstanceID"] = job.InstanceID
		fsbkConf["BackupID"] = job.BackupID
		break
	case "dbs":
		job.Backend = "dbs"
		var gwList []string
		gwList = append(gwList, u.Host)
		dbsConf["GatewayList"] = gwList
		/*
		 * support backward compatibility with no namspace
		 */
		if u.Path != "" {
			path, _ := ParsePath(u.Path)
			dbsConf["Namespace"] = path
		}
		dbsConf["InstanceID"] = job.InstanceID
		dbsConf["BackupID"] = job.BackupID
		break
	case "s3":
		job.Backend = "s3"
		s3Conf["Endpoint"] = u.Host
		path, _ := ParsePath(u.Path)
		s3Conf["Bucket"] = path
		s3Conf["InstanceID"] = job.InstanceID
		s3Conf["BackupID"] = job.BackupID
		s3Conf["ID"] = u.User.Username()
		s3Conf["Key"], _ = u.User.Password()
		break
	case "http":
		job.Backend = "http"
		httpConf["Endpoint"] = u.Host
		httpConf["InstanceID"] = job.InstanceID
		httpConf["BackupID"] = job.BackupID
		break
	default:
		break
	}

	externConf["task"] = job

	if req.Stage == "full" || req.Stage == "fullonly" || req.Stage == "block" {
		externConf["req"] = req
	}

	err = SendMsg(msg, notify)
	if err != nil {
		e := fmt.Errorf(`{"error": %s, "code": 1}`, err.Error())
		return e.Error(), e
	}
	return `{"error": null, "code": 0}`, nil
}

func StartFullBackupCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
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

	pbd := query.Get("pbd")

	_notify, ok := param["notify"]
	if !ok {
		err := errors.New(`{"error": "param.notify miss", "code":1}`)
		return err.Error(), err
	}
	notify, ok := _notify.(chan []byte)
	if !ok {
		err := errors.New(`{"error": "param.notify must be []byte", "code":1}`)
		return err.Error(), err
	}

	var req BackupRequest
	err := json.Unmarshal(data, &req)
	if err != nil {
		e := fmt.Errorf(`{"error": "invalid backup request, parse requst failed: %s", "code": 1002}`, err.Error())
		return e.Error(), e
	}

	externConf := make(map[string]interface{})
	dbsConf := make(map[string]interface{})
	pfsConf := make(map[string]interface{})
	fsbkConf := make(map[string]interface{})
	fspgConf := make(map[string]interface{})
	s3Conf := make(map[string]interface{})
	httpConf := make(map[string]interface{})
	localfsConf := make(map[string]interface{})
	externConf["dbs"] = dbsConf
	externConf["pfs"] = pfsConf
	externConf["fsbk"] = fsbkConf
	externConf["fspg"] = fspgConf
	externConf["s3"] = s3Conf
	externConf["localfs"] = localfsConf
	externConf["http"] = httpConf

	if pbd != "" {
		pfsConf["Pbd"] = pbd
	}
	pfsConf["Flags"] = "backup"

	var job BackupJob
	if req.BackupStorageSpace.Locations.Local.Path != "" {
		fsbkConf["Path"] = req.BackupStorageSpace.Locations.Local.Path
		fsbkConf["InstanceID"] = req.InstanceID
		fsbkConf["BackupID"] = req.BackupID
		job.Location = fmt.Sprintf("file:///%s?InstanceID=%s&BackupID=%s", fsbkConf["Path"], req.InstanceID, req.BackupID)
	}

	if req.BackupStorageSpace.Locations.DBS.Endpoint != "" {
		var gatewayList []string
		gatewayList = append(gatewayList, req.BackupStorageSpace.Locations.DBS.Endpoint)
		dbsConf["GatewayList"] = gatewayList
		dbsConf["Namespace"] = req.BackupStorageSpace.Locations.DBS.Namespace
		dbsConf["InstanceID"] = req.InstanceID
		dbsConf["BackupID"] = req.BackupID
		/*
		 * backward compatibility
		 */
		if req.BackupStorageSpace.Locations.DBS.Namespace != "" {
			job.Location = fmt.Sprintf("dbs://%s/%s?InstanceID=%s&BackupID=%s", gatewayList[0], req.BackupStorageSpace.Locations.DBS.Namespace, req.InstanceID, req.BackupID)
		} else {
			job.Location = fmt.Sprintf("dbs://%s?InstanceID=%s&BackupID=%s", gatewayList[0], req.InstanceID, req.BackupID)
		}
	}

	if req.BackupStorageSpace.Locations.S3.Endpoint != "" {
		s3Conf["Endpoint"] = req.BackupStorageSpace.Locations.S3.Endpoint
		s3Conf["Bucket"] = req.BackupStorageSpace.Locations.S3.Bucket
		s3Conf["InstanceID"] = req.InstanceID
		s3Conf["BackupID"] = req.BackupID
		s3Conf["ID"] = req.BackupStorageSpace.Locations.S3.Accesskey
		s3Conf["Key"] = req.BackupStorageSpace.Locations.S3.Secretkey
		s3Conf["Secure"] = req.BackupStorageSpace.Locations.S3.Secure
		secure := "false"
		if s3Conf["Secure"].(bool) {
			secure = "true"
		}
		job.Location = fmt.Sprintf("s3://%s:%s@%s/%s?InstanceID=%s&BackupID=%s&Secure=%s", s3Conf["ID"], s3Conf["Key"], s3Conf["Endpoint"], s3Conf["Bucket"], req.InstanceID, req.BackupID, secure)
	}

	if req.BackupStorageSpace.Locations.HTTP.Endpoint != "" {
		httpConf["Endpoint"] = req.BackupStorageSpace.Locations.HTTP.Endpoint
		httpConf["InstanceID"] = req.InstanceID
		httpConf["BackupID"] = req.BackupID
		job.Location = fmt.Sprintf("http://%s?InstanceID=%s&BackupID=%s", httpConf["Endpoint"], req.InstanceID, req.BackupID)
	}

	if req.DBClusterMetaDir != "" {
		localfsConf["Path"] = req.DBClusterMetaDir
	}

	job.BackupID = req.BackupID
	job.BackupJobID = req.BackupJobID
	job.InstanceID = req.InstanceID
	job.CallbackURL = req.CallbackURL
	job.DBClusterMetaDir = req.DBClusterMetaDir
	job.BackupFolder = req.BackupFolder
	job.MaxBackupSpeed = req.MaxBackupSpeed

	if strings.HasPrefix(req.Filesystem, "pfs.") {
		job.Frontend = "pfs"
	} else {
		job.Frontend = "fspg"
		fspgConf["Path"] = job.BackupFolder
	}

	job.Action = "backup"
	job.Backend = req.BackupStorageSpace.StorageType
	if job.Backend == "fs" {
		job.Backend = "fsbk"
	}
	job.Pbd = pbd
	job.WorkerCount = req.WorkerCount
	job.BackupNodes = req.BackupMachineList

	job.BackupAccount = req.BackupAccount
	job.MetaAccount = req.MetaAccount

	job.ManagerAddr = req.ManagerAddr
	job.UseBlock = req.UseBlock

	job.EnableEncryption = req.EnableEncryption
	job.EncryptionPassword = req.EncryptionPassword

	externConf["task"] = job

	if req.BackupType != BackupTypeFull {
		err := errors.New(`{"error": "operator not support now", "code": 1002}`)
		return err.Error(), err
	}

	plugin := "pgpipeline"
	if req.Filesystem == "pfs.chunk" {
		plugin = "pipeline"
	}

	msg := &Message{
		Cmd:        "backup",
		Plugin:     plugin,
		InstanceID: req.BackupID + req.InstanceID + req.BackupJobID,
		Data:       externConf,
	}

	err = SendMsg(msg, notify)
	if err != nil {
		e := fmt.Errorf(`{"error": %s, "code": 1}`, err.Error())
		return e.Error(), e
	}

	return `{"error": null, "code": 0}`, nil
}

func RenameWalCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
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

	_notify, ok := param["notify"]
	if !ok {
		err := errors.New(`{"error": "param.notify miss", "code":1}`)
		return err.Error(), err
	}
	notify, ok := _notify.(chan []byte)
	if !ok {
		err := errors.New(`{"error": "param.notify must be []byte", "code":1}`)
		return err.Error(), err
	}

	var req WalHelperRequest
	err := json.Unmarshal(data, &req)
	if err != nil {
		e := fmt.Errorf(`{"error": "invalid rename wal request, parse requst failed: %s", "code": 1002}`, err.Error())
		return e.Error(), e
	}

	externConf := make(map[string]interface{})
	externConf["req"] = req

	plugin := "wal"

	now := time.Now()
	id := now.Format("2006_01_02_15_04_05")

	msg := &Message{
		Cmd:        "renamewal",
		Plugin:     plugin,
		InstanceID: id,
		Data:       externConf,
	}

	err = SendMsg(msg, notify)
	if err != nil {
		e := fmt.Errorf(`{"error": %s, "code": 1}`, err.Error())
		return e.Error(), e
	}

	return `{"error": null, "code": 0}`, nil
}

func ForceRenameWalsCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
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

	_notify, ok := param["notify"]
	if !ok {
		err := errors.New(`{"error": "param.notify miss", "code":1}`)
		return err.Error(), err
	}
	notify, ok := _notify.(chan []byte)
	if !ok {
		err := errors.New(`{"error": "param.notify must be []byte", "code":1}`)
		return err.Error(), err
	}

	var req WalHelperRequest
	err := json.Unmarshal(data, &req)
	if err != nil {
		e := fmt.Errorf(`{"error": "invalid rename wal request, parse requst failed: %s", "code": 1002}`, err.Error())
		return e.Error(), e
	}

	externConf := make(map[string]interface{})
	externConf["req"] = req

	plugin := "wal"

	now := time.Now()
	id := now.Format("2006_01_02_15_04_05")

	msg := &Message{
		Cmd:        "renamewal",
		Plugin:     plugin,
		InstanceID: id,
		Data:       externConf,
	}

	err = SendMsg(msg, notify)
	if err != nil {
		e := fmt.Errorf(`{"error": %s, "code": 1}`, err.Error())
		return e.Error(), e
	}

	return `{"error": null, "code": 0}`, nil
}

func BlockHelperCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
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

	_notify, ok := param["notify"]
	if !ok {
		err := errors.New(`{"error": "param.notify miss", "code":1}`)
		return err.Error(), err
	}
	notify, ok := _notify.(chan []byte)
	if !ok {
		err := errors.New(`{"error": "param.notify must be []byte", "code":1}`)
		return err.Error(), err
	}

	var req WalHelperRequest
	err := json.Unmarshal(data, &req)
	if err != nil {
		e := fmt.Errorf(`{"error": "invalid rename wal request, parse requst failed: %s", "code": 1002}`, err.Error())
		return e.Error(), e
	}

	externConf := make(map[string]interface{})
	externConf["req"] = req

	plugin := "wal"

	now := time.Now()
	id := now.Format("2006_01_02_15_04_05")

	msg := &Message{
		Cmd:        req.Action,
		Plugin:     plugin,
		InstanceID: id,
		Data:       externConf,
	}

	err = SendMsg(msg, notify)
	if err != nil {
		e := fmt.Errorf(`{"error": %s, "code": 1}`, err.Error())
		return e.Error(), e
	}

	return `{"error": null, "code": 0}`, nil
}

func StartIncreBackupCallback(ctx interface{}, param map[string]interface{}) (interface{}, error) {
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

	pbd := query.Get("pbd")

	_notify, ok := param["notify"]
	if !ok {
		err := errors.New(`{"error": "param.notify miss", "code":1}`)
		return err.Error(), err
	}
	notify, ok := _notify.(chan []byte)
	if !ok {
		err := errors.New(`{"error": "param.notify must be []byte", "code":1}`)
		return err.Error(), err
	}

	var req BackupRequest
	err := json.Unmarshal(data, &req)
	if err != nil {
		e := fmt.Errorf(`{"error": "invalid backup request, parse requst failed: %s", "code": 1002}`, err.Error())
		return e.Error(), e
	}

	externConf := make(map[string]interface{})
	dbsConf := make(map[string]interface{})
	pfsConf := make(map[string]interface{})
	fsbkConf := make(map[string]interface{})
	fspgConf := make(map[string]interface{})
	s3Conf := make(map[string]interface{})
	httpConf := make(map[string]interface{})
	externConf["dbs"] = dbsConf
	externConf["pfs"] = pfsConf
	externConf["fsbk"] = fsbkConf
	externConf["fspg"] = fspgConf
	externConf["s3"] = s3Conf
	externConf["http"] = httpConf

	if pbd != "" {
		pfsConf["Pbd"] = pbd
	}
	pfsConf["Flags"] = "backup"

	var job BackupJob
	if req.BackupStorageSpace.Locations.Local.Path != "" {
		fsbkConf["Path"] = req.BackupStorageSpace.Locations.Local.Path
		fsbkConf["InstanceID"] = req.InstanceID
		fsbkConf["BackupID"] = req.BackupID
		job.Location = fmt.Sprintf("file:///%s?InstanceID=%s&BackupID=%s", fsbkConf["Path"], req.InstanceID, req.BackupID)
	}

	if req.BackupStorageSpace.Locations.DBS.Endpoint != "" {
		var gatewayList []string
		gatewayList = append(gatewayList, req.BackupStorageSpace.Locations.DBS.Endpoint)
		dbsConf["GatewayList"] = gatewayList
		dbsConf["Namespace"] = req.BackupStorageSpace.Locations.DBS.Namespace
		dbsConf["InstanceID"] = req.InstanceID
		dbsConf["BackupID"] = req.BackupID
		/*
		 * backward compatibility
		 */
		if req.BackupStorageSpace.Locations.DBS.Namespace != "" {
			job.Location = fmt.Sprintf("dbs://%s/%s?InstanceID=%s&BackupID=%s", gatewayList[0], req.BackupStorageSpace.Locations.DBS.Namespace, req.InstanceID, req.BackupID)
		} else {
			job.Location = fmt.Sprintf("dbs://%s?InstanceID=%s&BackupID=%s", gatewayList[0], req.InstanceID, req.BackupID)
		}
	}

	if req.BackupStorageSpace.Locations.S3.Endpoint != "" {
		s3Conf["Endpoint"] = req.BackupStorageSpace.Locations.S3.Endpoint
		s3Conf["Bucket"] = req.BackupStorageSpace.Locations.S3.Bucket
		s3Conf["InstanceID"] = req.InstanceID
		s3Conf["BackupID"] = req.BackupID
		s3Conf["ID"] = req.BackupStorageSpace.Locations.S3.Accesskey
		s3Conf["Key"] = req.BackupStorageSpace.Locations.S3.Secretkey
		s3Conf["Secure"] = req.BackupStorageSpace.Locations.S3.Secure
		secure := "false"
		if s3Conf["Secure"].(bool) {
			secure = "true"
		}
		job.Location = fmt.Sprintf("s3://%s:%s@%s/%s?InstanceID=%s&BackupID=%s&Secure=%s", s3Conf["ID"], s3Conf["Key"], s3Conf["Endpoint"], s3Conf["Bucket"], req.InstanceID, req.BackupID, secure)
	}

	if req.BackupStorageSpace.Locations.HTTP.Endpoint != "" {
		httpConf["Endpoint"] = req.BackupStorageSpace.Locations.HTTP.Endpoint
		httpConf["InstanceID"] = req.InstanceID
		httpConf["BackupID"] = req.BackupID
		job.Location = fmt.Sprintf("http://%s?InstanceID=%s&BackupID=%s", httpConf["Endpoint"], req.InstanceID, req.BackupID)
	}

	job.BackupID = req.BackupID
	job.BackupJobID = req.BackupJobID
	job.InstanceID = req.InstanceID
	job.CallbackURL = req.CallbackURL
	job.DBSCallbackURL = req.DBSCallbackURL
	job.BackupFolder = req.BackupFolder
	job.MaxBackupSpeed = req.MaxBackupSpeed

	if strings.HasPrefix(req.Filesystem, "pfs.") {
		job.Frontend = "pfs"
	} else {
		job.Frontend = "fspg"
		fspgConf["Path"] = job.BackupFolder
	}

	job.Action = "backup"
	job.Backend = req.BackupStorageSpace.StorageType
	if job.Backend == "fs" {
		job.Backend = "fsbk"
	}
	job.Pbd = pbd
	job.BackupNodes = req.BackupMachineList
	job.WorkerCount = req.WorkerCount

	job.BackupAccount = req.BackupAccount
	job.MetaAccount = req.MetaAccount

	job.PGType = req.PGType

	job.ManagerAddr = req.ManagerAddr
	job.DBNodes = req.DBNodes

	job.EnableEncryption = req.EnableEncryption
	job.EncryptionPassword = req.EncryptionPassword

	externConf["task"] = job

	if req.BackupType != BackupTypeIncremental {
		err := errors.New(`{"error": "operator not support now", "code": 1002}`)
		return err.Error(), err
	}

	plugin := "increpipeline"

	msg := &Message{
		Cmd:        "backup",
		Plugin:     plugin,
		InstanceID: req.BackupID + req.InstanceID + req.BackupJobID,
		Data:       externConf,
	}

	err = SendMsg(msg, notify)
	if err != nil {
		e := fmt.Errorf(`{"error": %s, "code": 1}`, err.Error())
		return e.Error(), e
	}

	return `{"error": null, "code": 0}`, nil
}

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	startBackupQueryPath   = "/manager/StartBackup"
	stopBackupQueryPath    = "/manager/StopBackup"
	statusBackupQueryPath  = "/manager/Describe"
	recoverBackupQueryPath = "/manager/Recovery"
)

var (
	BACKUP_JOB_ID                = "default"
	BACKUP_PG_TYPE               = "PolarDBStack"
	BACKUP_META_SOURCE           = "fs"
	BACKUP_FILESYSTEM            = "pfs.file"
	DEFAULT_INCREMENTAL_BACKUPID = "IncrememtalWals"
)

var (
	serviceUrlBase []string
	failedRR       int
)

type BackupConf struct {
	Service            string             `json:"Service"`
	InstanceID         string             `json:"InstanceID"`
	WorkerCount        int                `json:"WorkerCount"`
	UseBlock           bool               `json:"UseBlock"`
	BackupAccount      BackupAccount      `json:"BackupAccount"`
	Filesystem         string             `json:"Filesystem"`
	BackupStorageSpace BackupStorageSpace `json:"BackupStorageSpace"`
	BackupPBD          string             `json:"BackupPBD"`
	RecoverPBD         string             `json:"RecoverPBD"`
	RecoveryTime       int64              `json:"RecoveryTime"`
	MaxBackupSpeed     int                `json:"MaxBackupSpeed"`
	EnableEncryption   bool               `json:"EnableEncryption"`
	EncryptionPassword string             `json:"EncryptionPassword"`
}

type BackupRequest struct {
	BackupMachineList []string `json:"BackupMachineList"`
	InstanceID        string   `json:"InstanceID"`
	BackupID          string   `json:"BackupID"`
	BackupJobID       string   `json:"BackupJobID"`
	WorkerCount       int      `json:"WorkerCount"`
	BackupType        string   `json:"BackupType"`
	UseBlock          bool     `json:"UseBlock"`
	PGType            string   `json:"PGType"`
	StartOffset       int64    `json:"StartOffset"`
	CallbackURL       string   `json:"CallbackURL"`
	DBSCallbackURL    string   `json:"DBSCallbackURL"`
	//PFSMode            string   `json:"PFSMode"`
	PgDBConf           CommonDBConf       `json:"PgDBConf"`
	BackupAccount      BackupAccount      `json:"BackupAccount"`
	MetaAccount        MetaAccount        `json:"MetaAccount"`
	Filesystem         string             `json:"Filesystem"`
	BackupStorageSpace BackupStorageSpace `json:"BackupStorageSpace"`
	BackupPBD          string             `json:"BackupPBD"`
	BackupFolder       string             `json:"BackupFolder"`
	DBClusterMetaDir   string             `json:"DBClusterMetaDir"`
	MaxBackupSpeed     int                `json:"MaxBackupSpeed"`
	ManagerAddr        string             `json:"ManagerAddr"`
	DBNodes            []string           `json:"DBNodes"`
	EnableEncryption   bool               `json:"EnableEncryption"`
	EncryptionPassword string             `json:"EncryptionPassword"`
}

type RecoveryRequest struct {
	BackupMachineList  []string           `json:"BackupMachineList"`
	CallbackURL        string             `json:"CallbackURL"`
	InstanceID         string             `json:"InstanceID"` // target instance id
	BackupJobID        string             `json:"BackupJobID"`
	RecordID           string             `json:"RecordID"`
	WorkerCount        int                `json:"WorkerCount"`
	Full               BackupInfo         `json:"Full"`
	Incremental        BackupInfo         `json:"Incremental"`
	Stage              string             `json:"Stage"`
	RecoveryTime       int64              `json:"RecoveryTime"`
	RecoveryMode       string             `json:"RecoveryMode"`
	UseBlock           bool               `json:"UseBlock"`
	RecoveryFolder     string             `json:"RecoveryFolder"`
	Filesystem         string             `json:"Filesystem"`
	BackupStorageSpace BackupStorageSpace `json:"BackupStorageSpace"`
	BackupPBD          string             `json:"BackupPBD"`
	BackupAccount      BackupAccount      `json:"BackupAccount"`
	MetaAccount        MetaAccount        `json:"MetaAccount"`
	FullBackup         BackupMetaInfo     `json:"FullBackup"`
	BlockBackups       []BackupMetaInfo   `json:"BlockBackups"`
	BlockBackupIndex   int                `json:"BlockBackupIndex"`
	IncreBackups       []WalLogMetaInfo   `json:"IncreBackups"`
	BackupMetaSource   string             `json:"BackupMetaSource"`
	PGType             string             `json:"PGType"`
	DBClusterMetaDir   string             `json:"DBClusterMetaDir"`
	EnableEncryption   bool               `json:"EnableEncryption"`
	EncryptionPassword string             `json:"EncryptionPassword"`
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
	Endpoint  string `json:"Endpoint"`
	Namespace string `json:"Namespace"`
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

type BackupInfo struct {
	InstanceID  string `json:"InstanceID"`
	BackupID    string `json:"BackupID"`
	BackupJobID string `json:"BackupJobID"`
}

type BackupMetaInfo struct {
	File        string `json:"Files"` // meta file name
	StartTime   int64  `json:"StartTime"`
	EndTime     int64  `json:"EndTime"`
	Location    string `json:"Location"`
	InstanceID  string `json:"InstanceID"`
	BackupID    string `json:"BackupID"`
	BackupJobID string `json:"BackupJobID"`
	Status      string `json:"Status"`
}

type WalLogMetaInfo struct {
	File        string `json:"Files"`
	FileSize    int64  `json:"FileSize"`
	TimeMode    string `json:"TimeMode"`
	StartTime   int64  `json:"StartTime"`
	EndTime     int64  `json:"EndTime"`
	Location    string `json:"Location"`
	InstanceID  string `json:"InstanceID"`
	BackupID    string `json:"BackupID"`
	BackupJobID string `json:"BackupJobID"`
	Status      string `json:"Status"`
}

type OperationActionStatus struct {
	Code int
	Msg  string
}

func InitService(serviceUrl string) {
	if len(serviceUrlBase) > 0 {
		return
	}

	serviceUrlBase = make([]string, 1)
	serviceUrlBase[0] = "http://" + serviceUrl
}

func DoSimpleQuery(queryPath, queryMethod string, params ...string) (string, error) {
	// TODO: remember connection failed url base, and auto-select
	//   next one
	urlValue := serviceUrlBase[failedRR] + queryPath
	log.Debugf("urlValue: %v", urlValue)

	var (
		resp *http.Response
		err  error
	)

	dialTimeout := func(network, addr string) (net.Conn, error) {
		return net.DialTimeout(network, addr, 60*time.Second)
	}
	client := http.Client{Transport: &http.Transport{Dial: dialTimeout}}
	switch queryMethod {
	case http.MethodGet:
		resp, err = client.Get(urlValue)
	case http.MethodPost:
		postBody := ""
		contentType := "application/json"
		paramsLen := len(params)
		if paramsLen > 0 {
			postBody = params[0]
			if paramsLen > 1 && len(params[1]) > 0 {
				contentType = params[1]
			}
		}
		resp, err = client.Post(urlValue, contentType, strings.NewReader(postBody))
	}

	if err != nil {
		failedRR++
		if failedRR >= len(serviceUrlBase) {
			fmt.Printf("query to %s failed: %s\n", urlValue, err.Error())
			return "", err
		}
		log.Error("Error getting response. ", err, "; switch to next request target,", serviceUrlBase[failedRR])
		return DoSimpleQuery(queryPath, queryMethod, params...)
	}
	defer resp.Body.Close()
	// Read body from response
	body, err := ioutil.ReadAll(resp.Body)
	jsonBody := string(body)
	return jsonBody, err
}

func ServiceStartBackup(request BackupRequest) (string, error) {
	backupJsonBytes, _ := json.Marshal(request)
	backupJsonStr := string(backupJsonBytes)

	log.Debugf("curl -v '%s%s' -d '%s'", serviceUrlBase[failedRR], startBackupQueryPath, backupJsonStr)
	body, err := DoSimpleQuery(startBackupQueryPath, http.MethodPost, backupJsonStr)
	if err != nil {
		return body, err
	}

	var opStatus OperationActionStatus
	err = json.Unmarshal([]byte(body), &opStatus)
	if err != nil {
		return body, err
	}

	if opStatus.Code != 0 {
		return body, fmt.Errorf("%v", opStatus)
	}
	return body, err
}

func ServiceStopBackup(request BackupRequest) (string, error) {
	backupJsonBytes, _ := json.Marshal(request)
	backupJsonStr := string(backupJsonBytes)

	log.Debugf("curl -v '%s%s' -d '%s'", serviceUrlBase[failedRR], stopBackupQueryPath, backupJsonStr)
	body, err := DoSimpleQuery(stopBackupQueryPath, http.MethodPost, backupJsonStr)
	if err != nil {
		return body, err
	}

	var opStatus OperationActionStatus
	err = json.Unmarshal([]byte(body), &opStatus)
	if err != nil {
		return body, err
	}

	if opStatus.Code != 0 {
		return body, fmt.Errorf("%v", opStatus)
	}
	return body, err
}

func ServiceStatusBackup(backupType string) (string, error) {
	backupJsonStr := fmt.Sprintf("{\"BackupType\": \"%s\"}", backupType)

	log.Debugf("curl -v '%s%s' -d '%s'", serviceUrlBase[failedRR], statusBackupQueryPath, backupJsonStr)
	body, err := DoSimpleQuery(statusBackupQueryPath, http.MethodPost, backupJsonStr)
	if err != nil {
		return body, err
	}

	var opStatus OperationActionStatus
	err = json.Unmarshal([]byte(body), &opStatus)
	if err != nil {
		return body, err
	}

	if opStatus.Code != 0 {
		return body, fmt.Errorf("%v", opStatus)
	}
	return body, err
}

func ServiceRecoverBackup(request RecoveryRequest) (string, error) {
	recoveryJsonBytes, _ := json.Marshal(request)
	recoveryJsonStr := string(recoveryJsonBytes)

	log.Debugf("curl -v '%s%s' -d '%s'", serviceUrlBase[failedRR], recoverBackupQueryPath, recoveryJsonStr)
	body, err := DoSimpleQuery(recoverBackupQueryPath, http.MethodPost, recoveryJsonStr)
	if err != nil {
		return body, err
	}

	var opStatus OperationActionStatus
	err = json.Unmarshal([]byte(body), &opStatus)
	if err != nil {
		return body, err
	}

	if opStatus.Code != 0 {
		return body, fmt.Errorf("%v", opStatus)
	}
	return body, err
}

func er(errstr string) {
	fmt.Printf("%s\n", errstr)
	os.Exit(1)
}

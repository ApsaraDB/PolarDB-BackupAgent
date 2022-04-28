package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

const (
	DefaultWorkerCount        = 4
	MaxWorkerCount            = 128
	ChunkBufSize              = 20 * 1024 * 1024
	FileBufSize               = 256 * 1024
	MinBufSize                = 256 * 1024
	DefaultTarFileSize        = 20 * 1024 * 1024 * 1024
	DefaultServerPort         = "1888"
	DefaultNetInterface       = "bond0"
	DefaultBlockBackupDir     = "/tmp"
	DefaultBlockSize          = 8192
	DefaultBlockSuffix        = ".pg_o_block"
	TRUNCATE                  = 1
	NOTTRUNCATE               = 0
	DefaultEncryptionPassword = "PolarDB123456"
	USINGWALTIME              = "w"
	USINGREALTIME             = "r"
	SETCRC                    = true
	NOTSETCRC                 = false
	CHECKCRC                  = true
	NOTCHECKCRC               = false
	ENABLEMAXFILE             = true
	DISABLEMAXFILE            = false
)

type fileSystem struct {
	funcs  map[string]func(interface{}, map[string]interface{}) (interface{}, error)
	handle interface{}
	name   string
}

type EndPoint struct {
	Type       string `json:"Type"`
	InitFunc   string `json:"initFunc"`
	PluginName string `json:"PluginName"`
	ConfPath   string `json:"ConfPath"`
}

type BackupMeta struct {
	Files               []string `json:"Files"`
	PGFiles             []PGFile `json:"PGFiles"`
	BackupSize          string   `json:"BackupSize"`
	StartWalName        string   `json:"StartWalName"`
	LastCheckPointTime  string   `json:"LastCheckPointTime"`
	BackupEndTime       string   `json:"BackupEndTime"`
	LocalFile           string   `json:"LocalFile"`
	BackupStartTimeUnix int64    `json:"BackupStartTimeUnix"`
	BackupEndTimeUnix   int64    `json:"BackupEndTimeUnix"`
	UseBlock            bool     `json:"UseBlock"`
}

type backupConf struct {
	Compress           bool                `json:"Compress"`
	WorkerCount        int                 `json:"WorkerCount"`
	Name               string              `json:"Name"`
	Action             string              `json:"Action"`
	HostIP             string              `json:"HostIP"`
	StatusURL          string              `json:"StatusURL`
	BackupNodes        []string            `json:"BackupNodes"`
	Frontend           string              `json:"Frontend"`
	Backend            string              `json:"Backend"`
	Endpoints          map[string]EndPoint `json:"EndPoints"`
	ManagerAddr        []string            `json:"ManagerAddr"`
	Force              bool                `json:"Force"`
	IgnoreDirs         []string            `json:"IgnoreDirs"`
	IgnoreFiles        []string            `json:"IgnoreFiles"`
	InstanceID         string              `json:"InstanceID"`
	BackupID           string              `json:"BackupID"`
	PgDBConf           CommonDBConf        `json:"PgDBConf"`
	BackupMetaDir      string              `json:"BackupMetaDir"`
	LocalFilePlugin    string              `json:"LocalFilePlugin"`
	LocalFileDir       string              `json:"LocalFileDir"`
	RealTime           bool                `json:"RealTime"`
	WalStream          bool                `json:"WalStream"`
	EnableMeta         bool                `json:"EnableMeta"`
	UseWalTime         bool                `json:"UseWalTime"`
	MaxFileSize        int64               `json:"MaxFileSize"`
	MaxWalsRecord      int                 `json:"MaxWalsRecord"`
	MaxFullsRecord     int                 `json:"MaxFullsRecord"`
	RecoveryWals       []string            `json:"RecoveryWals"`
	MaxWalsBuffer      int                 `json:"MaxWalsBuffer"`
	BlockBackupDir     string              `json:"BlockBackupDir"`
	EnableBlockBackup  bool                `json:"EnableBlockBackup"`
	BlockServerPort    string              `json:"BlockServerPort"`
	EnableEncryption   bool                `json:"EnableEncryption"`
	EncryptionPassword string              `json:"EncryptionPassword"`
	SwitchWalPeroid    int                 `json:"SwitchWalPeroid"`
}

type BPSController struct {
	maxBps    int64
	curBps    int64
	lastBytes int64
	curBytes  int64
	lastTime  int64
	delayTime int
	step      int
	tokens    int64
	stop      bool
}

type ExtraInfo struct {
	MaxBackupSpeed int64
	Master         string
	ManagerAddr    string
}

type PGFile struct {
	Name string `json:"Name"`
	Crc  uint32 `json:"Crc"`
}

type BackupCtx struct {
	action      string
	lastAction  string
	queue       *BackupQueue
	workerCount int
	compress    bool
	usePFS      int
	walSegSize  int
	useBlock    bool

	hostip        string
	incrementalip string

	binPath string
	conf    *backupConf

	frontend  fileSystem
	backend   fileSystem
	localFile fileSystem
	metaFile  fileSystem
	funcs     *sync.Map

	stop       bool
	stopEvent  chan bool
	notify     chan []byte
	instanceid string

	manager string

	job BackupJob

	wallogs      chan LogFileInfo
	wallogsArray []LogFileInfo

	excludeFiles []string
	excludeDirs  []string
	logdir       string

	id      string
	cmdline bool
	inited  bool
	ref     int32

	status RunningStatus
	fail   int32
	force  bool

	logger       *log.Logger
	mutex        sync.Mutex
	fullComplete int32

	tarIndex     int32
	tarFiles     []string
	localTarFile string

	bkdbinfo *DBInfo
	mtdbinfo *DBInfo

	bpsController *BPSController

	lastStage string

	pgFiles        []PGFile
	crcMap         sync.Map
	blockBackupDir string

	sshcnn  *ssh.Client
	sftpcli *sftp.Client
	blockCtx *HTTPCtx

	enableBlockBackup  bool
	enableEncryption   bool
	encryptionPassword string

	minRecoveryTime int64
}

// func initDb(backupCtx *BackupCtx) error {
//     if backupCtx.action == "backup" {
//         dbInfo, err := initDBInfo(&backupCtx.conf.PgDBConf)
//         if err != nil {
//             return err
//         }
//         backupCtx.dbinfo = dbInfo
//     }
//     return nil
// }

type MyDiscard struct {
}

func (*MyDiscard) Read(buf []byte) (int, error) {
	sz := len(buf)
	return sz, nil
}

func convertUnixToTime(_t int64) string {
	var convert_t time.Time
	convert_t = time.Unix(_t, 0)
	t := convert_t.Format("2006-01-02 15:04:05")
	zone, _ := convert_t.Zone()
	return t + " " + zone
}

func checkAccountValid(account MetaAccount) bool {
	return account.Endpoint != "" && account.DBType != ""
}

func closeDB(dbInfo *DBInfo) {
	if dbInfo != nil {
		dbInfo.db.Close()
		dbInfo.isClose = true
	}
}

func handleHa(backupCtx *BackupCtx, dbInfo *DBInfo) {
	for {
		if dbInfo.isClose {
			break
		}

		ismaster, err := checkMaster(dbInfo)
		if err != nil {
			backupCtx.logger.Printf("[WARNING] check master failed: %s\n", err.Error())
		} else {
			if !ismaster {
				isFound := false
				needClose := true
				for _, node := range dbInfo.dbNodes {
					if needClose {
						dbInfo.db.Close()
					}

					dbInfo.endpoint = node
					err = openPsqlDB(dbInfo)
					if err != nil {
						backupCtx.logger.Printf("[WARNING] open psql db at %s failed: %s\n", node, err.Error())
						needClose = false
						continue
					}
					needClose = true

					ismaster, err := checkMaster(dbInfo)
					if err != nil {
						backupCtx.logger.Printf("[WARNING] check master failed: %s\n", err.Error())
					} else {
						if ismaster {
							backupCtx.logger.Printf("[INFO] master change to: %s\n", node)
							isFound = true
							break
						}
					}
				}
				if !isFound {
					backupCtx.logger.Printf("[WARNING] not found master\n")
				}
			}
		}

		time.Sleep(60 * time.Second)
	}
}

func initDbs(backupCtx *BackupCtx) (error, int) {
	backupCtx.logger.Printf("[INFO] init dbs\n")

	workcount := 0
	var err1, err2 error
	var bkdbInfo, mtdbInfo *DBInfo

	if backupCtx.conf.PgDBConf.Endpoint != "" && backupCtx.bkdbinfo == nil {
		backupCtx.logger.Printf("[INFO] try initPgsqlDBInfo\n")
		bkdbInfo, err1 = initPgsqlDBInfo(&backupCtx.conf.PgDBConf)
		if err1 == nil {
			backupCtx.bkdbinfo = bkdbInfo
			workcount += 1
			if len(backupCtx.job.DBNodes) > 1 {
				bkdbInfo.dbNodes = backupCtx.job.DBNodes
				go func() {
					handleHa(backupCtx, bkdbInfo)
				}()
			}
		} else {
			backupCtx.logger.Printf("[ERROR] initPgsqlDBInfo failed: %s\n", err1.Error())
		}
	}

	backupCtx.logger.Printf("[INFO] job.MetaAccount.DBType: %s, Endpoint:%s, Port:%s, Username:%s, Password:%s\n", backupCtx.job.MetaAccount.DBType, backupCtx.job.MetaAccount.Endpoint, backupCtx.job.MetaAccount.Port, backupCtx.job.MetaAccount.User, backupCtx.job.MetaAccount.Password)

	if checkAccountValid(backupCtx.job.MetaAccount) && backupCtx.mtdbinfo == nil {
		var dbconf CommonDBConf
		dbconf.Endpoint = backupCtx.job.MetaAccount.Endpoint
		dbconf.Port = backupCtx.job.MetaAccount.Port
		dbconf.Username = backupCtx.job.MetaAccount.User
		dbconf.Password = backupCtx.job.MetaAccount.Password
		dbconf.Database = backupCtx.job.MetaAccount.Database
		dbconf.ApplicationName = "default"

		if backupCtx.job.MetaAccount.DBType == "pgsql" {
			mtdbInfo, err2 = initPgsqlDBInfo(&dbconf)
			if err2 == nil {
				backupCtx.mtdbinfo = mtdbInfo
				workcount += 1
			} else {
				backupCtx.logger.Printf("[ERROR] initPgsqlDBInfo failed: %s\n", err2.Error())
			}
		} else if backupCtx.job.MetaAccount.DBType == "mysql" {
			mtdbInfo, err2 = initMysqlDBInfo(&dbconf)
			if err2 == nil {
				backupCtx.mtdbinfo = mtdbInfo
				workcount += 1
			} else {
				backupCtx.logger.Printf("[ERROR] initMysqlDBInfo failed: %s\n", err2.Error())
			}
		} else {
			err2 = errors.New("Not support db type")
		}

	}

	if err1 != nil || err2 != nil {
		var errstr string
		if err1 != nil {
			errstr = err1.Error()
		}
		if err2 != nil {
			errstr += err2.Error()
		}
		return errors.New(errstr), workcount
	}

	return nil, workcount
}

func initBash(backupCtx *BackupCtx) error {
	ex, err := os.Executable()
	if err != nil {
		return err
	}
	exPath := filepath.Dir(ex)

	shellPath := exPath + "/backup-initdb.sh"
	command := exec.Command(shellPath)
	err = command.Start()
	if nil != err {
		backupCtx.logger.Printf("[ERROR] excute backup-initdb.sh fail, %s\n", err.Error())
	}
	err = command.Wait()
	if nil != err {
		backupCtx.logger.Printf("[ERROR] execute backup-initdb.sh fail, %s\n", err.Error())
	}
	return err
}

func getActionFromTask(ctx *BackupCtx, param interface{}) (string, ExtraInfo, error) {
	var ei ExtraInfo

	m, ok := param.(map[string]interface{})
	if !ok {
		err := errors.New("param must be map[string]interface{}")
		return "", ei, err
	}

	var extern map[string]interface{}
	_extern, ok := m["extern"]
	if ok {
		extern, ok = _extern.(map[string]interface{})
		if !ok {
			err := errors.New("param.extern must be map[string]interface{}")
			return "", ei, err
		}
	} else {
		return "", ei, nil
	}

	_task, ok := extern["task"]
	if !ok {
		return "", ei, nil
	}
	task, ok := _task.(map[string]interface{})
	if !ok {
		ctx.logger.Printf("[ERROR] %s Message.Data[extern][task] must be map[string]interface{}\n", ctx.conf.Name)
		return "", ei, errors.New("param.extern.task must be map[string]interface{}")
	}

	content, err := json.MarshalIndent(&task, "", "\t")
	if err != nil {
		ctx.logger.Printf("[ERROR] %s Message.Data[extern][task] -> json failed\n", ctx.conf.Name)
		return "", ei, errors.New("param.extern.task -> json failed")
	}

	var job BackupJob
	err = json.Unmarshal(content, &job)
	if err != nil {
		ctx.logger.Printf("[ERROR] %s Message.Data[extern][task] must be BackupJob\n", ctx.conf.Name)
		return "", ei, errors.New("param.extern.task must be BackupJob failed")
	}

	ei.MaxBackupSpeed = int64(job.MaxBackupSpeed)
	ei.Master = job.Master
	ei.ManagerAddr = job.ManagerAddr
	return job.Action, ei, nil
}

func initTask(backupCtx *BackupCtx, extern map[string]interface{}) error {
	ctx := backupCtx

	ctx.logger.Printf("[INFO] %s init Message info\n", backupCtx.conf.Name)

	if !backupCtx.cmdline {
		backupCtx.localFile.name = backupCtx.conf.LocalFilePlugin
	}

	_task, ok := extern["task"]
	if !ok {
		ctx.logger.Printf("[INFO] %s no extern conf in Message, use default conf\n", backupCtx.conf.Name)
		backupCtx.frontend.name = backupCtx.conf.Frontend
		backupCtx.backend.name = backupCtx.conf.Backend
		return nil
	}
	task, ok := _task.(map[string]interface{})
	if !ok {
		ctx.logger.Printf("[ERROR] %s Message.Data[extern][task] must be map[string]interface{}\n", backupCtx.conf.Name)
		return errors.New("param.extern.task must be map[string]interface{}")
	}

	content, err := json.MarshalIndent(&task, "", "\t")
	if err != nil {
		ctx.logger.Printf("[ERROR] %s Message.Data[extern][task] -> json failed\n", backupCtx.conf.Name)
		return errors.New("param.extern.task -> json failed")
	}

	var job BackupJob
	err = json.Unmarshal(content, &job)
	if err != nil {
		ctx.logger.Printf("[ERROR] %s Message.Data[extern][task] must be BackupJob\n", backupCtx.conf.Name)
		return errors.New("param.extern.task must be BackupJob failed")
	}
	backupCtx.status.BackupMachineList = job.BackupNodes
	backupCtx.job = job

	_req, ok := extern["req"]
	if ok {
		req, ok := _req.(map[string]interface{})
		if !ok {
			ctx.logger.Printf("[ERROR] %s Message.Data[extern][req] must be map[string]interface{}\n", backupCtx.conf.Name)
			return errors.New("param.extern.req must be map[string]interface{}")
		}
		content, err := json.MarshalIndent(&req, "", "\t")
		if err != nil {
			ctx.logger.Printf("[ERROR] %s Message.Data[extern][req] -> json failed\n", backupCtx.conf.Name)
			return errors.New("param.extern.task -> json failed")
		}

		var rreq RecoveryRequest
		err = json.Unmarshal(content, &rreq)
		if err != nil {
			ctx.logger.Printf("[ERROR] %s Message.Data[extern][req] must be RecoveryRequest\n", backupCtx.conf.Name)
			return errors.New("param.extern.req must be RecoveryRequest failed")
		}
		err = NotifyManagerRecordRecoveryCallback(backupCtx, &rreq)
		if err != nil {
			return err
		}
	}

	ctx.logger.Printf("[INFO] %s init Message success, new task:\n%s\n", backupCtx.conf.Name, string(content))
	return nil
}

func finiDependence(backupCtx *BackupCtx) error {
	return nil
}

func initDependence(backupCtx *BackupCtx, extern map[string]interface{}) error {
	var err error

	pluginInitParams := make(map[string]map[string]interface{})
	pluginConfMap := make(map[string]map[string]interface{})
	for _, endpoint := range backupCtx.conf.Endpoints {
		endpoint.ConfPath = backupCtx.binPath + "/" + endpoint.ConfPath
		backupCtx.logger.Printf("[INFO] %s init dependence conf %s\n", backupCtx.conf.Name, endpoint.ConfPath)
		content, err := ioutil.ReadFile(endpoint.ConfPath)
		if err != nil {
			backupCtx.logger.Printf("[ERROR] %s read dependence conf %s, failed: %s\n", backupCtx.conf.Name, endpoint.ConfPath, err)
			return fmt.Errorf("read [%s] conf file [%s] failed: %s", endpoint.PluginName, endpoint.ConfPath, err.Error())
		}
		initParam := make(map[string]interface{})
		initParam["conf"] = content

		confMap := make(map[string]interface{})
		err = json.Unmarshal(content, &confMap)
		if err != nil {
			backupCtx.logger.Printf("[ERROR] %s parse dependence conf %s, failed: %s, conf\n%s\n", backupCtx.conf.Name, endpoint.ConfPath, err, string(content))
			return err
		}
		pluginInitParams[endpoint.PluginName] = initParam
		pluginConfMap[endpoint.PluginName] = confMap
	}

	// override origin conf
	backupCtx.logger.Printf("[INFO] override origin configuration\n")
	if extern != nil {
		for name, _paramExtern := range extern {
			paramExtern, ok := _paramExtern.(map[string]interface{})
			if !ok {
				backupCtx.logger.Printf("[ERROR] %s dependence plugin [%s] conf format invalid\n", backupCtx.conf.Name, name)
				return fmt.Errorf("%s extern conf is invalid", name)
			}

			initParam, ok := pluginInitParams[name]
			var content []byte
			if ok {
				confMap := pluginConfMap[name]
				for k, v := range paramExtern {
					confMap[k] = v
				}
				content, err = json.MarshalIndent(confMap, "", "\t")
				initParam["conf"] = content
			}
			if err != nil {
				backupCtx.logger.Printf("[ERROR] %s set dependence plugin [%s] conf failed: %s\n", backupCtx.conf.Name, name, err.Error())
				return err
			}
			backupCtx.logger.Printf("[INFO] %s dependence plugin [%s] overrided conf: \n%s\n", backupCtx.conf.Name, name, content)
		}
	}

	backupCtx.logger.Printf("[INFO] init frontend\n")
	if _, ok := pluginInitParams[backupCtx.frontend.name]; !ok {
		backupCtx.logger.Printf("[ERROR] %s dependence plugin [%s] conf miss\n", backupCtx.conf.Name, backupCtx.frontend.name)
		return fmt.Errorf("plugin %s miss conf", backupCtx.frontend.name)
	}
	backupCtx.frontend.handle, err = backupCtx.frontend.funcs["init"](nil, pluginInitParams[backupCtx.frontend.name])
	if err != nil {
		backupCtx.logger.Printf("[ERROR] %s init dependence plugin [%s] failed: %s\n", backupCtx.conf.Name, backupCtx.frontend.name, err.Error())
		return err
	}

	backupCtx.logger.Printf("[INFO] init backend\n")
	if _, ok := pluginInitParams[backupCtx.backend.name]; !ok {
		backupCtx.logger.Printf("[ERROR] %s dependence plugin [%s] conf miss\n", backupCtx.conf.Name, backupCtx.backend.name)
		backupCtx.frontend.funcs["fini"](backupCtx.frontend.handle, nil)
		return fmt.Errorf("plugin %s miss conf", backupCtx.backend.name)
	}
	backupCtx.backend.handle, err = backupCtx.backend.funcs["init"](nil, pluginInitParams[backupCtx.backend.name])
	if err != nil {
		backupCtx.frontend.funcs["fini"](backupCtx.frontend.handle, nil)
		backupCtx.logger.Printf("[ERROR] %s init dependence plugin [%s] failed: %s\n", backupCtx.conf.Name, backupCtx.backend.name, err.Error())
		return err
	}

	backupCtx.logger.Printf("[INFO] init meta file\n")
	if backupCtx.metaFile.name != "" {
		_conf := pluginInitParams[backupCtx.metaFile.name]["conf"]
		conf, ok := _conf.([]byte)
		if !ok {
			backupCtx.frontend.funcs["fini"](backupCtx.frontend.handle, nil)
			backupCtx.logger.Printf("[INFO] %s unparse content, failed: %s, conf\n%s\n", backupCtx.conf.Name, err, string(conf))
		}
		confMap := make(map[string]interface{})
		err = json.Unmarshal(conf, &confMap)
		if err != nil {
			backupCtx.frontend.funcs["fini"](backupCtx.frontend.handle, nil)
			backupCtx.logger.Printf("[INFO] %s unparse content, failed: %s, conf\n%s\n", backupCtx.conf.Name, err, string(conf))
			return err
		}
		confMap["BackupID"] = "fullmetas"
		content, err := json.MarshalIndent(confMap, "", "\t")
		if err != nil {
			backupCtx.frontend.funcs["fini"](backupCtx.frontend.handle, nil)
			backupCtx.logger.Printf("[INFO] %s make content, failed: %s\n", backupCtx.conf.Name, err)
			return err
		}
		pluginInitParams[backupCtx.metaFile.name]["conf"] = content
		backupCtx.metaFile.handle, err = backupCtx.metaFile.funcs["init"](nil, pluginInitParams[backupCtx.metaFile.name])
		if err != nil {
			backupCtx.frontend.funcs["fini"](backupCtx.frontend.handle, nil)
			backupCtx.logger.Printf("[ERROR] %s init dependence plugin [%s] failed: %s\n", backupCtx.conf.Name, backupCtx.metaFile.name, err.Error())
			return err
		}
	}

	backupCtx.logger.Printf("[INFO] init local file\n")
	if backupCtx.localFile.name != "" {
		if _, ok := pluginInitParams[backupCtx.localFile.name]; !ok {
			backupCtx.logger.Printf("[WARNING] %s dependence plugin [%s] conf miss\n", backupCtx.conf.Name, backupCtx.localFile.name)
		} else {
			backupCtx.localFile.handle, err = backupCtx.localFile.funcs["init"](nil, pluginInitParams[backupCtx.localFile.name])
			if err != nil {
				backupCtx.frontend.funcs["fini"](backupCtx.frontend.handle, nil)
				backupCtx.backend.funcs["fini"](backupCtx.backend.handle, nil)
				backupCtx.logger.Printf("[ERROR] %s init dependence plugin [%s] failed: %s\n", backupCtx.conf.Name, backupCtx.localFile.name, err.Error())
				return err
			}
		}
	}

	backupCtx.inited = true

	return nil
}

func loadFunction(method, module string, imports *sync.Map) (func(interface{}, map[string]interface{}) (interface{}, error), error) {
	_moduleExports, ok := imports.Load(module)
	if !ok {
		return nil, fmt.Errorf("cannot find method %s in module %s", method, module)
	}
	moduleExports, ok := _moduleExports.(map[string]interface{})
	if !ok {
		return nil, errors.New("imports.module must be map[string]interface{}")
	}
	_callback, ok := moduleExports[method]
	if !ok {
		return nil, fmt.Errorf("cannot find function %s.%s in imports", method, module)
	}
	callback, ok := _callback.(func(interface{}, map[string]interface{}) (interface{}, error))
	if !ok {
		return nil, fmt.Errorf("function %s.%s in imports must be func(interface{},map[string]interface{})(interface{}, error)", method, module)
	}
	return callback, nil
}

func initFunction(backupCtx *BackupCtx, plugin string, endpoint string) error {
	if plugin == "" {
		return errors.New("plugin is null when try to initfunction for it")
	}

	mapping, err := loadFunction("ExportMapping", plugin, backupCtx.funcs)
	if err != nil {
		return err
	}

	entry, err := mapping(nil, nil)
	if endpoint == "Frontend" {
		backupCtx.frontend.funcs, _ = entry.(map[string]func(interface{}, map[string]interface{}) (interface{}, error))
		backupCtx.frontend.name = plugin
		if plugin == "pfs" {
			backupCtx.usePFS = 1
		}
	} else if endpoint == "Backend" {
		backupCtx.backend.funcs, _ = entry.(map[string]func(interface{}, map[string]interface{}) (interface{}, error))
		backupCtx.backend.name = plugin
	} else if endpoint == "LocalFile" {
		backupCtx.localFile.funcs, _ = entry.(map[string]func(interface{}, map[string]interface{}) (interface{}, error))
		backupCtx.localFile.name = plugin
	} else if endpoint == "MetaFile" {
		backupCtx.metaFile.funcs, _ = entry.(map[string]func(interface{}, map[string]interface{}) (interface{}, error))
		backupCtx.metaFile.name = plugin
	} else {
		return fmt.Errorf("init Function: invalid endpoint: %s", err.Error())
	}
	return nil
}

func getLocation(ctx *BackupCtx) (string, error) {
	var location string
	switch ctx.conf.Backend {
	case "fs", "fs1", "fs2":
		path, err := ctx.backend.funcs["getpath"](ctx.backend.handle, nil)
		if err != nil {
			return "", errors.New("get path fail")
		}
		location = fmt.Sprintf("file:///%s?InstanceID=%s&BackupID=%s", path, ctx.conf.InstanceID, ctx.conf.BackupID)
		break
	case "dbs":
		path, err := ctx.backend.funcs["getpath"](ctx.backend.handle, nil)
		if err != nil {
			return "", errors.New("get path fail")
		}
		location = fmt.Sprintf("dbs://%s?InstanceID=%s&BackupID=%s", path, ctx.conf.InstanceID, ctx.conf.BackupID)
		break
	case "s3":
		path, err := ctx.backend.funcs["getpath"](ctx.backend.handle, nil)
		if err != nil {
			return "", errors.New("get path fail")
		}
		location = fmt.Sprintf("s3://%s?InstanceID=%s&BackupID=%s", path, ctx.conf.InstanceID, ctx.conf.BackupID)
		break
	case "http":
		path, err := ctx.backend.funcs["getpath"](ctx.backend.handle, nil)
		if err != nil {
			return "", errors.New("get path fail")
		}
		location = fmt.Sprintf("http://%s?InstanceID=%s&BackupID=%s", path, ctx.conf.InstanceID, ctx.conf.BackupID)
		break
	default:
		return "", errors.New("Not support backend type")
	}
	return location, nil
}

func NotifyManagerBlockHelperCallback(ctx *BackupCtx, action string) error {
	var whReq WalHelperRequest
	whReq.InstanceID = ctx.job.InstanceID
	whReq.BlockBackupDir = ctx.blockBackupDir
	whReq.Action = action

	content, err := json.MarshalIndent(whReq, "", "\t")
	if err != nil {
		ctx.logger.Printf("[ERROR] %s req -> json failed: %s\n", ctx.conf.Name, err.Error())
		return errors.New("NotifyManagerBlockHelperCallback, req -> json failed")
	}

	r := bytes.NewReader(content)
	URL := "http://" + ctx.manager
	u, err := url.Parse(URL)
	if err != nil {
		return err
	}

	u.Path = path.Join(u.Path, "manager/BlockHelper")

	ctx.logger.Printf("[INFO] %s request %s to rename wal\n", ctx.conf.Name, u.String())
	resp, err := http.Post(u.String(), "application/json", r)
	if err != nil {
		ctx.logger.Printf("[INFO] %s request [%s] failed: %s\n", ctx.conf.Name, u.String(), err.Error())
		return err
	}
	defer resp.Body.Close()
	respContent, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		ctx.logger.Printf("[ERROR] manager read [%s] response failed: %s\n", u.String(), err.Error())
		return err
	}
	var respInfo ResponseData
	err = json.Unmarshal(respContent, &respInfo)
	if err != nil {
		ctx.logger.Printf("[ERROR] manager [%s] response parse failed: %s\n", u.String(), err.Error())
		return err
	}
	if respInfo.Code != 0 {
		ctx.logger.Printf("[ERROR] manager [%s] execute failed: %s\n", u.String(), respInfo.Error)
		return err
	}

	return nil
}

func NotifyManagerRenameWalCallback(ctx *BackupCtx, logfile LogFileInfo) error {
	var whReq WalHelperRequest
	whReq.BackupFolder = ctx.job.BackupFolder
	whReq.CurLogFile = logfile
	whReq.Action = "renamewal"

	content, err := json.MarshalIndent(whReq, "", "\t")
	if err != nil {
		ctx.logger.Printf("[ERROR] %s req -> json failed: %s\n", ctx.conf.Name, err.Error())
		return errors.New("NotifyManagerRecordRecoveryCallback, req -> json failed")
	}

	r := bytes.NewReader(content)
	URL := "http://" + ctx.manager
	u, err := url.Parse(URL)
	if err != nil {
		return err
	}

	u.Path = path.Join(u.Path, "manager/RenameWal")

	ctx.logger.Printf("[INFO] %s request %s to rename wal\n", ctx.conf.Name, u.String())
	resp, err := http.Post(u.String(), "application/json", r)
	if err != nil {
		ctx.logger.Printf("[INFO] %s request [%s] failed: %s\n", ctx.conf.Name, u.String(), err.Error())
		return err
	}
	defer resp.Body.Close()
	respContent, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		ctx.logger.Printf("[ERROR] manager read [%s] response failed: %s\n", u.String(), err.Error())
		return err
	}
	var respInfo ResponseData
	err = json.Unmarshal(respContent, &respInfo)
	if err != nil {
		ctx.logger.Printf("[ERROR] manager [%s] response parse failed: %s\n", u.String(), err.Error())
		return err
	}
	if respInfo.Code != 0 {
		ctx.logger.Printf("[ERROR] manager [%s] execute failed: %s\n", u.String(), respInfo.Error)
		return err
	}

	return nil
}

func NotifyManagerRecordRecoveryCallback(ctx *BackupCtx, req *RecoveryRequest) error {
	req.RecordID = ctx.job.InstanceID + ctx.job.BackupID + ctx.job.BackupJobID

	content, err := json.MarshalIndent(*req, "", "\t")
	if err != nil {
		ctx.logger.Printf("[ERROR] %s req -> json failed: %s\n", ctx.conf.Name, err.Error())
		return errors.New("NotifyManagerRecordRecoveryCallback, req -> json failed")
	}

	r := bytes.NewReader(content)
	URL := "http://" + ctx.manager
	u, err := url.Parse(URL)
	if err != nil {
		return err
	}

	u.Path = path.Join(u.Path, "manager/RecordRecovery")

	ctx.logger.Printf("[INFO] %s request %s to record recovery\n", ctx.conf.Name, u.String())
	resp, err := http.Post(u.String(), "application/json", r)
	if err != nil {
		ctx.logger.Printf("[INFO] %s request [%s] failed: %s\n", ctx.conf.Name, u.String(), err.Error())
		return err
	}
	defer resp.Body.Close()
	respContent, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		ctx.logger.Printf("[ERROR] manager read [%s] response failed: %s\n", u.String(), err.Error())
		return err
	}
	var respInfo ResponseData
	err = json.Unmarshal(respContent, &respInfo)
	if err != nil {
		ctx.logger.Printf("[ERROR] manager [%s] response parse failed: %s\n", u.String(), err.Error())
		return err
	}
	if respInfo.Code != 0 {
		ctx.logger.Printf("[ERROR] manager [%s] execute failed: %s\n", u.String(), respInfo.Error)
		return err
	}

	return nil
}

func NotifyManagerMultiStageRecoveryCallback(ctx *BackupCtx, stage string) {
	content, err := json.MarshalIndent(&ctx.status, "", "\t")
	if err != nil {
		ctx.logger.Printf("[ERROR] %s status -> json failed: %s\n", ctx.conf.Name, err.Error())
		return
	}

	r := bytes.NewReader(content)
	URL := "http://" + ctx.manager
	u, err := url.Parse(URL)
	if err != nil {
		return
	}
	u.Path = path.Join(u.Path, ctx.conf.StatusURL)
	ctx.logger.Printf("[INFO] %s request %s to report backup status\n", ctx.conf.Name, u.String())
	resp, err := http.Post(u.String()+"?multistage=true", "application/json", r)
	if err != nil {
		ctx.logger.Printf("[INFO] %s request [%s] failed: %s\n", u.String(), ctx.conf.Name, err.Error())
		return
	}
	resp.Body.Close()
}

func NotifyManagerCallback(ctx *BackupCtx, stage string) {
	if ctx.cmdline {
		return
	}

	if ctx.job.RecoveryType != "fullonly" && ctx.status.Action == "restore" {
		NotifyManagerMultiStageRecoveryCallback(ctx, stage)
		return
	}

	content, err := json.MarshalIndent(&ctx.status, "", "\t")
	if err != nil {
		ctx.logger.Printf("[ERROR] %s status -> json failed: %s\n", ctx.conf.Name, err.Error())
		return
	}

	r := bytes.NewReader(content)
	URL := "http://" + ctx.manager
	u, err := url.Parse(URL)
	if err != nil {
		return
	}
	u.Path = path.Join(u.Path, ctx.conf.StatusURL)
	if ctx.lastStage != stage {
		ctx.logger.Printf("[INFO] %s request %s to report backup status: %s\n", ctx.conf.Name, u.String(), stage)
		ctx.lastStage = stage
	}
	resp, err := http.Post(u.String(), "application/json", r)
	if err != nil {
		ctx.logger.Printf("[INFO] %s request [%s] failed: %s\n", u.String(), ctx.conf.Name, err.Error())
		return
	}
	resp.Body.Close()
}

func CopyBufferN(dst io.Writer, src io.Reader, buf []byte, n int64, bpsc *BPSController) (written int64, crc uint32, err error) {
	if n == 0 {
		return 0, 0, nil
	}

	size := len(buf)
	count := n / int64(size)
	left := n % int64(size)

	var i int64
	for i = 0; i < count; i++ {
		nr, er := src.Read(buf)
		if nr > 0 {
			crc = crc32.Update(crc, crc32.IEEETable, buf[:nr])
			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = errors.New("write error:" + ew.Error())
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = errors.New("read error:" + er.Error())
			}
			break
		}
		if bpsc != nil {
			bpsc.accumulateBytes(int64(nr))
			bpsc.adjustBPS()
		}
	}

	if err != nil {
		return written, crc, err
	}

	for left > 0 {
		buf = buf[0:left]
		nr, er := src.Read(buf)
		if nr > 0 {
			crc = crc32.Update(crc, crc32.IEEETable, buf[:nr])
			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = ew
			}
			if nr != nw {
				fmt.Printf("!! short write, nr:%d nw:%d\n", nr, nw)
				err = io.ErrShortWrite
			}
		}

		if er != nil {
			if er != io.EOF {
				err = er
				break
			}
		}

		if bpsc != nil {
			bpsc.accumulateBytes(int64(nr))
		}

		left = left - int64(nr)
		if nr == 0 {
			break
		}
	}

	return written, crc, err
}

func Copy(dst io.Writer, src io.Reader) (int64, uint32, error) {
	buf := make([]byte, 16*1024)
	return CopyBuffer(dst, src, buf, nil)
}

func CopyBuffer(dst io.Writer, src io.Reader, buf []byte, bpsc *BPSController) (written int64, crc uint32, err error) {
	if buf == nil {
		return 0, 0, errors.New("buf cannot be nil")
	}
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			crc = crc32.Update(crc, crc32.IEEETable, buf[:nr])
			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = errors.New("write error:" + ew.Error())
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = errors.New("read error:" + er.Error())
			}
			break
		}
		if bpsc != nil {
			bpsc.accumulateBytes(int64(nr))
			bpsc.adjustBPS()
		}
	}
	return written, crc, err
}

func CopyBufferAlign(dst io.Writer, src io.Reader, buf []byte) (int64, uint32, error) {
	var written, readed int64
	var err error
	var offset, total, i int64
	var crc uint32

	for {
		rsize, er := src.Read(buf[offset:])
		if rsize > 0 {
			crc = crc32.Update(crc, crc32.IEEETable, buf[:rsize])
			readed += int64(rsize)
			total = offset + int64(rsize)
			n := total / 4096
			size := n * 4096
			offset = total - size
			if size > 0 {
				blockbuf := buf[0:size]
				wsize, ew := dst.Write(blockbuf)
				if ew != nil {
					err = ew
					break
				}
				if wsize > 0 {
					written += int64(wsize)
				}
				for i = 0; i < offset; i++ {
					buf[i] = buf[size+i]
				}
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	if offset > 0 {
		return written, crc, fmt.Errorf("invalid write length left: %d", offset)
	}

	if written != readed {
		return written, crc, fmt.Errorf("mismatch copy size,  write length: %d, read length: %d", written, readed)
	}
	return written, crc, err
}

func IsNotExist(err error) bool {
	return err == syscall.ENOENT || os.IsNotExist(err)
}

func UnlinkFile(fs fileSystem, filename string) (interface{}, error) {
	param := make(map[string]interface{})
	param["name"] = filename
	ret, err := fs.funcs["unlink"](fs.handle, param)

	if err != nil {
		return nil, err
	}

	return ret, nil
}

func ExistFile(fs fileSystem, filename string) (interface{}, error) {
	param := make(map[string]interface{})
	param["path"] = filename
	ret, err := fs.funcs["exist"](fs.handle, param)

	if err != nil {
		return nil, err
	}

	return ret, nil
}

func OpenFile(fs fileSystem, filename string) (interface{}, error) {
	param := make(map[string]interface{})
	param["name"] = filename
	param["flags"] = uint(00)
	param["mode"] = uint(0400)
	fi, err := fs.funcs["open"](fs.handle, param)

	if err != nil {
		return nil, err
	}

	return fi, nil
}

func CreateFile(fs fileSystem, filename string) (interface{}, error) {
	param := make(map[string]interface{})
	param["name"] = filename
	fi, err := fs.funcs["create"](fs.handle, param)
	if err != nil {
		return nil, err
	}

	return fi, nil
}

func Close(fs fileSystem, fp interface{}) (interface{}, error) {
	param := make(map[string]interface{})
	param["file"] = fp
	return fs.funcs["close"](fs.handle, param)
}

func (bpsc *BPSController) init(maxbps int) {
	bpsc.step = 1
	bpsc.maxBps = int64(maxbps) * 1024 * 1024
	bpsc.delayTime = 10
	bpsc.tokens = bpsc.maxBps / FileBufSize
	bpsc.stop = false
}

func (bpsc *BPSController) updateBPS(maxbps int64) {
	if maxbps == 0 {
		bpsc.maxBps = 0
		bpsc.stop = true
		return
	}

	bpsc.maxBps = maxbps * 1024 * 1024
	bpsc.stop = false
}

func (bpsc *BPSController) adjustBPS() {
	if bpsc.maxBps == 0 {
		return
	}

	for {
		for bpsc.tokens <= 0 {
			time.Sleep(time.Duration(10) * time.Microsecond)
			if bpsc.stop {
				break
			}
		}

		newTokens := atomic.AddInt64(&bpsc.tokens, -1)
		if newTokens >= 0 {
			break
		}

		if bpsc.stop {
			break
		}
	}
}

func (bpsc *BPSController) accumulateBytes(bytenum int64) {
	atomic.AddInt64(&bpsc.curBytes, bytenum)
}

func (bpsc *BPSController) adjustCore() {
	if bpsc.maxBps == 0 {
		return
	}

	curTime := time.Now().UnixNano()
	curBytes := bpsc.curBytes
	if bpsc.lastTime == 0 {
		bpsc.lastTime = curTime
		return
	}
	gap := curTime - bpsc.lastTime

	curBPS := (curBytes - bpsc.lastBytes) * 1e9 / gap

	bpsc.curBps = curBPS
	bpsc.lastTime = curTime
	bpsc.lastBytes = curBytes

	bpsc.tokens = bpsc.maxBps / FileBufSize
}

func CapturePanic(ctx *BackupCtx) {
	defer func() {
		if r := recover(); r != nil {
			errStr := string(debug.Stack())
			fmt.Printf("backupctl panic from:\n%s", errStr)
			ctx.logger.Printf("backupctl panic from:\n%s", errStr)
		}
	}()
}

func errorEnhance(err error) error {
	var ehError error
	switch err.Error() {
	case "The specified key does not exist.":
		ehError = errors.New(err.Error() + " Please check if the file or backup set exist.")
		break
	default:
		ehError = err
		break
	}

	return ehError
}

func ParsePath(raw string) (string, error) {
	newUrl, _ := url.Parse(raw)
	path := newUrl.Path
	return path[1:], nil
}

func getCurrentWalFile(backupCtx *BackupCtx) (string, int64, error) {
	rows, err := queryDB(backupCtx.bkdbinfo, "select * FROM pg_walfile_name_offset(pg_current_wal_flush_lsn())")
	if err != nil {
		backupCtx.logger.Printf("[ERROR] get current wal file name failed. err: %s\n", err.Error())
		return "", int64(0), err
	}
	defer rows.Close()

	var walfile string
	var offset int64

	if rows.Next() {
		if err := rows.Scan(&walfile, &offset); err != nil {
			backupCtx.logger.Printf("[ERROR] scan current wal file result failed. err: %s\n", err.Error())
			return "", int64(0), err
		}

		return walfile, offset, nil
	}

	backupCtx.logger.Printf("[ERROR] current wal file name is empty")
	return "", int64(0), errors.New("current wal file name is empty")
}

func ReadDir(ctx *BackupCtx, fs fileSystem, dir string) ([]string, error) {
	params := make(map[string]interface{})
	params["path"] = dir

	_files, err := fs.funcs["readdir"](fs.handle, params)
	if err != nil {
		ctx.status.Error = err.Error()
		ctx.status.Stage = "Failed"
		ctx.logger.Printf("[ERROR] increpipeline readdir failed. dir[%s] err[%s]\n", params["path"].(string), err.Error())
		NotifyManagerCallback(ctx, ctx.status.Stage)
		return nil, err
	}
	files, ok := _files.([]string)
	if !ok {
		ctx.status.Error = err.Error()
		ctx.status.Stage = "Failed"
		NotifyManagerCallback(ctx, ctx.status.Stage)
		ctx.logger.Printf("[ERROR] increpipeline readdir must return []string\n")
		return nil, errors.New("readir result must be []string")
	}

	return files, nil
}

func prepareBlockBackupDir(ctx *BackupCtx) error {
	dir := ctx.blockBackupDir + "/polar_block_backup/" + ctx.status.InstanceID
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0700)
	}

	ctx.blockBackupDir = dir

	return err
}

func createSSHConnect(ip string) (*ssh.Client, error) {
	privateKeyFile := "/root/.ssh/id_rsa"
	remoteIP := ip + ":22"
	user := "root"
	privateKeyBytes, err := ioutil.ReadFile(privateKeyFile)
	if err != nil {
		return nil, err
	}

	key, err := ssh.ParsePrivateKey(privateKeyBytes)
	if err != nil {
		return nil, err
	}

	config := &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			// Use the PublicKeys method for remote authentication.
			ssh.PublicKeys(key),
		},
		// using InsecureIgnoreHostKey() for testing purposes
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         1 * time.Second,
	}

	sshcnn, err := ssh.Dial("tcp", remoteIP, config)
	if err != nil {
		return nil, err
	}
	return sshcnn, nil
}

func getIncrementalBackupIp(ctx *BackupCtx) (string, error) {
	var whReq WalHelperRequest
	whReq.InstanceID = ctx.job.InstanceID
	content, err := json.MarshalIndent(whReq, "", "\t")
	if err != nil {
		ctx.logger.Printf("[ERROR] %s req -> json failed: %s\n", ctx.conf.Name, err.Error())
		return "", errors.New("getIncrementalBackupIp, req -> json failed")
	}

	r := bytes.NewReader(content)
	URL := "http://" + ctx.manager
	u, err := url.Parse(URL)
	if err != nil {
		return "", err
	}

	u.Path = path.Join(u.Path, "manager/QueryIncrementalNode")

	ctx.logger.Printf("[INFO] %s request %s to record recovery\n", ctx.conf.Name, u.String())
	resp, err := http.Post(u.String(), "application/json", r)
	if err != nil {
		ctx.logger.Printf("[INFO] %s request [%s] failed: %s\n", ctx.conf.Name, u.String(), err.Error())
		return "", err
	}
	defer resp.Body.Close()
	respContent, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		ctx.logger.Printf("[ERROR] manager read [%s] response failed: %s\n", u.String(), err.Error())
		return "", err
	}
	var respInfo ResponseData
	err = json.Unmarshal(respContent, &respInfo)
	if err != nil {
		ctx.logger.Printf("[ERROR] manager [%s] response parse failed: %s, origin content:%s\n", u.String(), err.Error(), string(respContent))
		return "", err
	}
	if respInfo.Code != 0 {
		ctx.logger.Printf("[ERROR] manager [%s] execute failed: %s\n", u.String(), respInfo.Error)
		return "", errors.New(respInfo.Error)
	}

	return respInfo.Data, nil
}

func storageToMap(storage *BackupStorageSpace, confMap *(map[string]interface{})) {
	switch storage.StorageType {
	case "fs":
		(*confMap)["path"] = storage.Locations.Local.Path
		break
	case "dbs":
		var gwlist []string
		gwlist = append(gwlist, storage.Locations.DBS.Endpoint)
		(*confMap)["GatewayList"] = gwlist
		(*confMap)["Namespace"] = storage.Locations.DBS.Namespace
		break
	case "s3":
		(*confMap)["Endpoint"] = storage.Locations.S3.Endpoint
		(*confMap)["ID"] = storage.Locations.S3.Accesskey
		(*confMap)["Key"] = storage.Locations.S3.Secretkey
		(*confMap)["Bucket"] = storage.Locations.S3.Bucket
		(*confMap)["Secure"] = storage.Locations.S3.Secure
		break
	case "http":
		(*confMap)["Endpoint"] = storage.Locations.HTTP.Endpoint
		break
	default:
		break
	}
}

func storageToLocation(storage *BackupStorageSpace, instanceid string, backupid string) string {
	location := ""
	switch storage.StorageType {
	case "fs":
		location = fmt.Sprintf("file:///%s?InstanceID=%s&BackupID=%s", storage.Locations.Local.Path, instanceid, backupid)
		break
	case "dbs":
		if storage.Locations.DBS.Namespace != "" {
			location = fmt.Sprintf("dbs://%s/%s?InstanceID=%s&BackupID=%s", storage.Locations.DBS.Endpoint, storage.Locations.DBS.Namespace, instanceid, backupid)
		} else {
			location = fmt.Sprintf("dbs://%s?InstanceID=%s&BackupID=%s", storage.Locations.DBS.Endpoint, instanceid, backupid)
		}
		break
	case "s3":
		secure := "false"
		if storage.Locations.S3.Secure {
			secure = "true"
		}
		location = fmt.Sprintf("s3://%s:%s@%s/%s?InstanceID=%s&BackupID=%s&Secure=%s", storage.Locations.S3.Accesskey, storage.Locations.S3.Secretkey, storage.Locations.S3.Endpoint, storage.Locations.S3.Bucket, instanceid, backupid, secure)
		break
	case "http":
		location = fmt.Sprintf("http://%s?InstanceID=%s&BackupID=%s", storage.Locations.HTTP.Endpoint, instanceid, backupid)
		break
	default:
		break
	}
	return location
}
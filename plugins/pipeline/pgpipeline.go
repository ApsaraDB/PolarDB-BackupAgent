package main

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pierrec/lz4"
)

const (
	META_EXT = "pg_o_backup_meta"
)

const (
	F_OK = 0 /* Test for existence.  */
)

func PluginInit(ctx interface{}) (interface{}, error) {
	backupCtx := &BackupCtx{}

	m, ok := ctx.(map[string]interface{})
	if !ok {
		return nil, errors.New("invalid init ctx")
	}

	_notify, ok := m["notify"]
	if !ok {
		return nil, errors.New("ctx.notify miss")
	}
	notify, ok := _notify.(chan []byte)
	if !ok {
		return nil, errors.New("ctx.notify must be chan []byte")
	}

	_logger, ok := m["logger"]
	if !ok {
		return nil, errors.New("ctx.logger miss")
	}

	logger, ok := _logger.(*log.Logger)
	if !ok {
		return nil, errors.New("ctx.logger must be *log.Logger")
	}

	_stop, ok := m["stop"]
	if !ok {
		return nil, errors.New("ctx.stop miss")
	}
	stop, ok := _stop.(chan bool)
	if !ok {
		return nil, errors.New("ctx.stop must be chan bool")
	}

	_cmdline, ok := m["cmdline"]
	if !ok {
		return nil, errors.New("ctx.cmdline miss")
	}
	cmdline, ok := _cmdline.(bool)
	if !ok {
		return nil, errors.New("ctx.stop must be bool")
	}

	_conf, ok := m["conf"]
	if !ok {
		return nil, errors.New("ctx.conf miss")
	}
	conf, ok := _conf.([]byte)
	if !ok {
		return nil, errors.New("ctx.conf must be []byte")
	}

	_binPath, ok := m["binpath"]
	if !ok {
		return nil, errors.New("ctx.binPath miss")
	}
	binPath, ok := _binPath.(string)
	if !ok {
		return nil, errors.New("ctx.binPath must be string")
	}

	var initConf backupConf
	err := json.Unmarshal(conf, &initConf)
	if err != nil {
		return nil, errors.New("parse plugin conf failed:" + err.Error())
	}

	action := initConf.Action
	if action != "restore" && action != "backup" {
		return nil, errors.New("ctx.action's value must be `backup` or `restore`")
	}

	// worker
	workerCount := initConf.WorkerCount
	if workerCount < 0 || workerCount > MaxWorkerCount {
		return nil, errors.New("ctx.worker must in [1, 128]")
	}
	if workerCount == 0 {
		workerCount = DefaultWorkerCount
	}

	_funcs, ok := m["imports"]
	if !ok {
		return nil, errors.New("ctx.imports miss")
	}
	funcs, ok := _funcs.(*sync.Map)
	if !ok {
		return nil, errors.New("ctx.imports must be *sync.Map")
	}

	backupCtx.funcs = funcs
	backupCtx.frontend = fileSystem{}
	backupCtx.backend = fileSystem{}

	backupCtx.stopEvent = stop
	backupCtx.queue = NewSingleNodeDispatchTask()
	backupCtx.notify = notify
	backupCtx.stop = false
	backupCtx.cmdline = cmdline
	backupCtx.compress = initConf.Compress
	backupCtx.workerCount = workerCount
	backupCtx.conf = &initConf
	backupCtx.binPath = binPath
	backupCtx.force = backupCtx.conf.Force
	backupCtx.action = backupCtx.conf.Action
	backupCtx.hostip = backupCtx.conf.HostIP
	backupCtx.id = backupCtx.conf.BackupID
	backupCtx.enableBlockBackup = backupCtx.conf.EnableBlockBackup
	backupCtx.enableEncryption = backupCtx.conf.EnableEncryption
	backupCtx.encryptionPassword = backupCtx.conf.EncryptionPassword
	backupCtx.crcMap = sync.Map{}

	if backupCtx.encryptionPassword == "" {
		backupCtx.encryptionPassword = DefaultEncryptionPassword
	}

	if backupCtx.conf.MaxFileSize == 0 {
		backupCtx.conf.MaxFileSize = DefaultTarFileSize
	}

	if backupCtx.conf.BlockBackupDir != "" {
		backupCtx.blockBackupDir = backupCtx.conf.BlockBackupDir
	} else {
		backupCtx.blockBackupDir = DefaultBlockBackupDir
	}

	backupCtx.logdir = "pg_wal"
	backupCtx.excludeDirs = []string{"pg_wal", "pg_logindex", "polar_dma"} //[]string{"pg_stat_tmp", "pg_replslot", "pg_dynshmem", "pg_notify", "pg_serial", "pg_snapshots", "pg_subtrans", "pg_wal"}
	if len(backupCtx.conf.IgnoreDirs) > 0 {
		backupCtx.excludeDirs = append(backupCtx.excludeDirs, backupCtx.conf.IgnoreDirs...)
	}
	backupCtx.excludeFiles = []string{} // []string{"postgresql.auto.conf.tmp", "current_logfiles.tmp", "pg_internal.init", "backup_label", "pg_stat_tmp", "pg_replslot", "pg_dynshmem", "pg_notify", "pg_serial", "pg_snapshots", "pg_subtrans"}
	if len(backupCtx.conf.IgnoreFiles) > 0 {
		backupCtx.excludeFiles = append(backupCtx.excludeDirs, backupCtx.conf.IgnoreFiles...)
	}

	backupCtx.logger = logger

	if len(backupCtx.conf.ManagerAddr) > 0 {
		backupCtx.manager = backupCtx.conf.ManagerAddr[0]
		if len(strings.Split(backupCtx.manager, ":")) == 1 {
			backupCtx.manager = backupCtx.manager + ":" + DefaultServerPort
		}
	}

	return backupCtx, nil
}

func PluginRun(ctx interface{}, param interface{}) error {
	var err error
	backupCtx, ok := ctx.(*BackupCtx)
	if !ok {
		err = errors.New("ctx must be *BackupCtx")
		return err
	}

	CapturePanic(backupCtx)

	if backupCtx == nil {
		err = errors.New("ctx must not be empty")
		return err
	}

	if backupCtx.stop {
		err = errors.New("backupjob in current plugin recieve stop yet")
		return err
	}

	action, extraInfo, err := getActionFromTask(backupCtx, param)
	if err != nil {
		return err
	}
	if action == "limit" {
		// backupCtx.bpsController.maxBps = int64(extraInfo.MaxBackupSpeed) * 1024 * 1024
		backupCtx.bpsController.updateBPS(extraInfo.MaxBackupSpeed)
		backupCtx.logger.Printf("[INFO] max backup speed set to %d MB/s\n", extraInfo.MaxBackupSpeed)
		return nil
	}

	if extraInfo.ManagerAddr != "" {
		backupCtx.manager = extraInfo.ManagerAddr
		backupCtx.logger.Printf("[INFO] manager set to %s\n", backupCtx.manager)
	}

	atomic.AddInt32(&backupCtx.ref, 1)

	defer func() {
		atomic.AddInt32(&backupCtx.ref, -1)
		backupCtx.stop = true
	}()

	m, ok := param.(map[string]interface{})
	if !ok {
		err = errors.New("param must be map[string]interface{}")
		return err
	}

	//init frontend handle
	var extern map[string]interface{}
	_extern, ok := m["extern"]
	if ok {
		extern, ok = _extern.(map[string]interface{})
		if !ok {
			err = errors.New("param.extern must be map[string]interface{}")
			return err
		}
	}

	err = initTask(backupCtx, extern)
	if err != nil {
		backupCtx.logger.Printf("[ERROR] pgpipeline init task failed, exit now\n")
		backupCtx.status.Error = err.Error()
		NotifyManagerCallback(backupCtx, "Failed")
		return err
	}

	if backupCtx.job.WorkerCount != 0 {
		backupCtx.workerCount = backupCtx.job.WorkerCount
	}

	// override action
	if backupCtx.job.Action != "" {
		backupCtx.status.Action = backupCtx.job.Action
		backupCtx.conf.Action = backupCtx.job.Action
		backupCtx.action = backupCtx.job.Action
		backupCtx.status.Location = backupCtx.job.Location
		backupCtx.useBlock = backupCtx.job.UseBlock
		backupCtx.status.UseBlock = backupCtx.job.UseBlock
		backupCtx.enableEncryption = backupCtx.job.EnableEncryption
		if backupCtx.job.EncryptionPassword != "" {
			backupCtx.encryptionPassword = backupCtx.job.EncryptionPassword
		}
	}

	if backupCtx.action == "stop" {
		backupCtx.stop = true
		backupCtx.status.Stage = "Stop"
		NotifyManagerCallback(backupCtx, "Stop")
		return nil
	}

	defer func() {
		if !backupCtx.cmdline && backupCtx.job.Action == "restore" {
			var file *os.File
			if err == nil {
				file, err = os.Create("fullfinish")
				if err != nil {
					backupCtx.logger.Printf("[ERROR] increpipeline create finish file failed: %s\n", err.Error())
				}
			} else {
				file, err = os.Create("error")
			}
			if file != nil {
				file.Close()
			}
		}
	}()

	backupCtx.status.Stage = "Preparing"
	backupCtx.status.Node = backupCtx.hostip
	backupCtx.status.StartTime = time.Now().Unix()
	backupCtx.status.Backend = backupCtx.job.Backend
	backupCtx.status.Type = "Full"

	backupCtx.status.BackupID = backupCtx.job.BackupID
	backupCtx.status.BackupJobID = backupCtx.job.BackupJobID
	backupCtx.status.InstanceID = backupCtx.job.InstanceID
	backupCtx.status.Callback = backupCtx.job.CallbackURL

	if backupCtx.job.BackupAccount.Endpoint != "" {
		backupCtx.conf.PgDBConf.Endpoint = strings.Split(backupCtx.job.BackupAccount.Endpoint, ",")[0]
		backupCtx.conf.PgDBConf.Port = backupCtx.job.BackupAccount.Port
		backupCtx.conf.PgDBConf.Username = backupCtx.job.BackupAccount.User
		backupCtx.conf.PgDBConf.Password = backupCtx.job.BackupAccount.Password
	}
	backupCtx.status.PgDBConf = backupCtx.conf.PgDBConf
	backupCtx.status.MetaAccount = backupCtx.job.MetaAccount

	if backupCtx.id == "" && backupCtx.job.BackupID == "" {
		now := time.Now()
		backupCtx.id = now.Format("2006_01_02_15_04_05")
	} else {
		backupCtx.id = backupCtx.job.BackupID + backupCtx.job.InstanceID
	}

	backupCtx.status.ID = getMetaName(backupCtx)

	frontEnd := backupCtx.conf.Frontend
	if backupCtx.job.Frontend != "" {
		frontEnd = backupCtx.job.Frontend
	}
	err = initFunction(backupCtx, frontEnd, "Frontend")
	if err != nil {
		backupCtx.status.Error = err.Error()
		NotifyManagerCallback(backupCtx, "Failed")
		return err
	}
	backEnd := backupCtx.conf.Backend
	if backupCtx.job.Backend != "" {
		backEnd = backupCtx.job.Backend
	}
	err = initFunction(backupCtx, backEnd, "Backend")
	if err != nil {
		backupCtx.status.Error = err.Error()
		NotifyManagerCallback(backupCtx, "Failed")
		return err
	}
	err = initFunction(backupCtx, backEnd, "MetaFile")
	if err != nil {
		backupCtx.status.Error = err.Error()
		NotifyManagerCallback(backupCtx, "Failed")
		return err
	}

	localFiles := backupCtx.conf.LocalFilePlugin
	if localFiles != "" {
		err = initFunction(backupCtx, localFiles, "LocalFile")
		if err != nil {
			backupCtx.status.Error = err.Error()
			NotifyManagerCallback(backupCtx, "Failed")
			return err
		}
	}

	if backupCtx.action == "backup" {
		err, _ = initDbs(backupCtx)
		if err != nil {
			backupCtx.logger.Printf("[ERROR] pgpipeline init db failed, exit now\n")
			backupCtx.status.Error = err.Error()
			NotifyManagerCallback(backupCtx, "Failed")
			return err
		}
		defer func() {
			closeDB(backupCtx.bkdbinfo)
			closeDB(backupCtx.mtdbinfo)
		}()
		err = initFullBackupFlag(backupCtx.bkdbinfo)
		if err != nil {
			backupCtx.logger.Printf("[ERROR] pgpipeline initFullBackupFlag failed, exit now\n")
			backupCtx.status.Error = err.Error()
			NotifyManagerCallback(backupCtx, "Failed")
			return err
		}
		if backupCtx.useBlock {
			err = initBlockBackupFlag(backupCtx.bkdbinfo)
			if err != nil {
				backupCtx.logger.Printf("[ERROR] pgpipeline initBlockBackupFlag failed, exit now\n")
				backupCtx.status.Error = err.Error()
				NotifyManagerCallback(backupCtx, "Failed")
				return err
			}
		}
	}

	backupCtx.logger.Printf("[INFO] pgpipeline start [report] goroutine, report addr: %s\n", backupCtx.manager)
	go func() {
		stop := false
		for !stop {
			if backupCtx.stop {
				backupCtx.logger.Printf("[INFO] pgpipeline %s stop, [report] goroutine exit\n", backupCtx.action)
				stop = true
			}
			if backupCtx.bpsController != nil {
				updateBackedSize(backupCtx, backupCtx.bpsController.curBytes)
			}
			backupCtx.logger.Printf("[INFO] pgpipeline [report] %s cmd status: %s\n", backupCtx.action, backupCtx.status.Stage)
			NotifyManagerCallback(backupCtx, backupCtx.status.Stage)
			time.Sleep(5 * time.Second)
		}
	}()

	backupCtx.logger.Printf("[INFO] pgpipeline start bpscontroller\n")
	backupCtx.bpsController = &BPSController{}
	backupCtx.bpsController.init(backupCtx.job.MaxBackupSpeed)
	go func() {
		stop := false
		for !stop {
			if backupCtx.stop {
				backupCtx.logger.Printf("[INFO] pgpipeline %s stop, [report] goroutine exit\n", backupCtx.action)
				stop = true
				backupCtx.bpsController.stop = true
			}
			backupCtx.bpsController.adjustCore()
			// backupCtx.logger.Printf("[INFO] pgpipeline curent bps: %d MB/s\n", backupCtx.bpsController.curBps / 1048576)
			time.Sleep(1000 * time.Millisecond)
		}
	}()

	go func() {
		stop := false
		var lastBytes, curBps int64
		curBps = 0
		lastBytes = 0
		period := 5
		b2mb := 1048576
		for !stop {
			if backupCtx.stop {
				backupCtx.logger.Printf("[INFO] pgpipeline %s stop, [report] goroutine exit\n", backupCtx.action)
				stop = true
			}
			// backupCtx.bpsController.adjustCore()
			// backupCtx.logger.Printf("[INFO] pgpipeline curent bps: %d MB/s\n", backupCtx.bpsController.curBps / 1048576)
			if lastBytes == 0 {
				lastBytes = backupCtx.bpsController.curBytes
			} else {
				curBytes := backupCtx.bpsController.curBytes
				curBps = (curBytes - lastBytes) / int64(period*b2mb)
				lastBytes = curBytes
			}
			backupCtx.logger.Printf("[INFO] pgpipeline curent bps: %d MB/s\n", curBps)
			time.Sleep(time.Duration(period) * time.Second)
		}
	}()

	backupCtx.logger.Printf("[INFO] pgpipeline init dependence conf begin...\n")
	err = initDependence(backupCtx, extern)
	if err != nil {
		backupCtx.logger.Printf("[ERROR] pgpipeline init dependence failed: %s\n", err.Error())
		return err
	}
	backupCtx.logger.Printf("[INFO] pgpipeline init dependence success\n")

	if backupCtx.cmdline {
		backupCtx.status.InstanceID = backupCtx.conf.InstanceID
		backupCtx.status.BackupID = backupCtx.conf.BackupID
		backupCtx.status.BackupJobID = "default"
		backupCtx.id = backupCtx.conf.BackupID + backupCtx.conf.InstanceID
		backupCtx.status.Location, err = getLocation(backupCtx)
		if err != nil {
			backupCtx.logger.Printf("[ERROR] pgpipeline get location failed: %s\n", err.Error())
			return err
		}
	}

	if backupCtx.enableBlockBackup {
		err = prepareBlockBackupDir(backupCtx)
		if err != nil {
			backupCtx.logger.Printf("[ERROR] prepare block backup dir failed: %s\n", err.Error())
			return err
		}
	}

	tb := time.Now()

	action = backupCtx.action
	if action == "backup" {
		backupCtx.logger.Printf("[INFO] pgpipeline do backup now\n")
		// compress info need to send to backend for restore
		err = backupAction(backupCtx)
		backupCtx.status.EndTime = time.Now().Unix()
	} else if action == "restore" {
		backupCtx.logger.Printf("[INFO] pgpipeline do restore now\n")
		err = restoreAction(backupCtx)
		backupCtx.status.EndTime = time.Now().Unix()
	} else {
		err = errors.New("receive action:" + action)
		backupCtx.logger.Printf("[ERROR] pgpipeline receive action: %s\n", backupCtx.action)
	}

	te := time.Now()
	backupCtx.logger.Printf("[INFO] pgpipeline do %s time: %s\n", action, te.Sub(tb))

	if err != nil {
		backupCtx.logger.Printf("[ERROR] pgpipeline %s failed: %s\n", backupCtx.action, err.Error())
		backupCtx.status.Error = err.Error()
		backupCtx.status.Stage = "Failed"
		if backupCtx.cmdline {
			os.Exit(1)
		}
	} else {
		backupCtx.status.Stage = "Finished"
	}

	finiDependence(backupCtx)
	backupCtx.logger.Printf("[INFO] pgpipelinefini dependence plugin\n")

	return err
}

func PluginExit(ctx interface{}) error {
	backupCtx, ok := ctx.(*BackupCtx)
	if !ok {
		return errors.New("invalid backupCtx")
	}

	if backupCtx != nil {
		if atomic.LoadInt32(&backupCtx.ref) == 0 && backupCtx.inited {
			backupCtx.backend.funcs["fini"](backupCtx.backend.handle, nil)
			backupCtx.frontend.funcs["fini"](backupCtx.frontend.handle, nil)
			if backupCtx.localFile.name != "" {
				backupCtx.localFile.funcs["fini"](backupCtx.localFile.handle, nil)
			}
		}
	}
	return nil
}

func parallelBackupWorkflow(backupCtx *BackupCtx, useBlock bool, worker int, wait *sync.WaitGroup) {
	for i := 0; i < worker; i++ {
		wait.Add(1)
		go func() {
			err := _backupWorkflow(backupCtx, useBlock, SETCRC, ENABLEMAXFILE, "")
			if err != nil {
				atomic.AddInt32(&backupCtx.fail, 1)
			}
			wait.Done()
		}()
	}
}

func parallelRestoreWorkflow(backupCtx *BackupCtx, worker int, wait *sync.WaitGroup) {
	for i := 0; i < worker; i++ {
		wait.Add(1)
		go func() {
			err := restoreWorkflow(backupCtx)
			if err != nil {
				backupCtx.logger.Printf("restore error: %s\n", err.Error())
				atomic.AddInt32(&backupCtx.fail, 1)
			}
			wait.Done()
		}()
	}
}

func listdirRecursion(path string, param map[string]interface{}, fs *fileSystem) ([]string, error) {
	if fs.handle == nil {
		return nil, errors.New("fs.handle is nil")
	}
	param["path"] = path
	files, err := fs.funcs["readdir_r"](fs.handle, param)
	if err != nil {
		return nil, err
	}
	return files.([]string), nil
}

func needBackup(dir string, excludes []string) bool {
	for _, path := range excludes {
		if dir == path {
			return false
		}
	}
	return true
}

func getBackupFiles(ctx *BackupCtx, dirs []string, param map[string]interface{}) ([]string, error) {
	files := make([]string, 0, 1024)
	for _, p := range dirs {
		param["path"] = p
		isexist, err := ctx.frontend.funcs["exist"](ctx.frontend.handle, param)
		if err != nil {
			ctx.logger.Printf("[ERROR] pgpipeline exist %s failed: %s\n", param["path"], err.Error())
			return nil, err
		}
		if !isexist.(bool) {
			continue
		}
		status, err := ctx.frontend.funcs["isdir"](ctx.frontend.handle, param)
		if err != nil {
			return nil, err
		}
		if status.(bool) {
			if needBackup(p, ctx.excludeDirs) {
				f, err := listdirRecursion(p, param, &ctx.frontend)
				if err != nil {
					ctx.logger.Printf("[ERROR] pgpipeline listdirRecursion %s failed: %s\n", p, err.Error())
					return nil, err
				}
				files = append(files, f...)
			}

			ctx.pgFiles = append(ctx.pgFiles, PGFile{p, 0})
		} else {
			if needBackup(p, ctx.excludeFiles) {
				files = append(files, p)
			}
		}
	}

	for _, file := range files {
		ctx.pgFiles = append(ctx.pgFiles, PGFile{file, 0})
	}

	return files, nil
}

func restoreWorkflow(ctx *BackupCtx) error {
	err := _restoreWorkflow(ctx, CHECKCRC)
	return err
}

func extractRegularFile(ctx *BackupCtx, filename string, hdr *tar.Header, curInput io.Reader, checkCRC bool) (int64, error) {
	ctx.logger.Printf("[INFO] pgpipeline extract regular file: %s\n", filename)

	buf := make([]byte, FileBufSize)

	param := make(map[string]interface{})

	param["path"] = filename
	ret, err := ctx.frontend.funcs["exist"](ctx.frontend.handle, param)
	if err != nil {
		ctx.logger.Printf("[ERROR] pgpipeline exist %s failed: %s\n", param["path"], err.Error())
		return 0, err
	}

	param["name"] = filename

	if ret.(bool) {
		_, err := ctx.frontend.funcs["unlink"](ctx.frontend.handle, param)
		if err != nil {
			ctx.logger.Printf("[ERROR] pgpipgline frontend unlink %s failed: %s\n", filename, err.Error())
			return 0, err
		}
	}

	param["mode"] = uint(0600)
	file, err := ctx.frontend.funcs["create"](ctx.frontend.handle, param)
	if err != nil {
		ctx.logger.Printf("[ERROR] pgpipgline frontend create %s failed: %s\n", filename, err.Error())
		return 0, err
	}

	if hdr.Size > 0 {
		param["file"] = file
		param["size"] = int64(hdr.Size)
		_, err = ctx.frontend.funcs["fallocate"](ctx.frontend.handle, param)
		if err != nil {
			ctx.logger.Printf("[ERROR] pgpipgline frontend fallocate %s failed: %s\n", filename, err.Error())
			ctx.frontend.funcs["close"](ctx.frontend.handle, param)
			return 0, err
		}
		ctx.logger.Printf("[INFO] pgpipgline frontend fallocate %s done\n", filename)
	}

	writer := file.(io.Writer)
	wsz, crc, err := CopyBuffer(writer, curInput, buf, ctx.bpsController)
	param["file"] = file
	ctx.frontend.funcs["close"](ctx.frontend.handle, param)
	if err != nil {
		ctx.logger.Printf("[ERROR] pgpipgline write file %s failed: %s\n", filename, err.Error())
		return 0, err
	}

	if checkCRC {
		oricrc, ok := ctx.crcMap.Load(filename)
		if ok {
			if oricrc != crc {
				err = errors.New("pgpipgline check crc of file failed")
				ctx.logger.Printf("[ERROR] pgpipgline check crc of file %s failed, origin crc: %d, new crc: %d\n", filename, oricrc, crc)
				return 0, err
			} else {
				ctx.logger.Printf("[INFO] pgpipgline check crc of file %s done\n", filename)
			}
		} else {
			ctx.logger.Printf("[WARNING] pgpipgline not found crc of file %s\n", filename)
		}
	}

	return wsz, nil
}

func extractBlockFile(ctx *BackupCtx, filename string, hdr *tar.Header, curInput io.Reader, checkCRC bool) (int64, error) {
	ctx.logger.Printf("[INFO] pgpipeline extract block file: %s\n", filename)

	buf := make([]byte, FileBufSize)
	hlen := int64(unsafe.Sizeof(BackupPageHeader{}))

	filename = strings.TrimSuffix(filename, DefaultBlockSuffix)

	frontendParam := make(map[string]interface{})

	frontendParam["path"] = filename
	ret, err := ctx.frontend.funcs["exist"](ctx.frontend.handle, frontendParam)
	if err != nil {
		ctx.logger.Printf("[ERROR] pgpipeline exist %s failed: %s\n", frontendParam["path"], err.Error())
		return 0, err
	}

	frontendParam["name"] = filename
	var frontendFile interface{}

	if ret.(bool) {
		frontendParam["flags"] = uint(01)
		frontendParam["mode"] = uint(0400)
		frontendFile, err = ctx.frontend.funcs["open"](ctx.frontend.handle, frontendParam)
		if err != nil {
			ctx.logger.Printf("[ERROR] pgpipgline frontend open %s failed: %s\n", filename, err.Error())
			return 0, err
		}
	} else {
		frontendParam["mode"] = uint(0600)
		frontendFile, err = ctx.frontend.funcs["create"](ctx.frontend.handle, frontendParam)
		if err != nil {
			ctx.logger.Printf("[ERROR] pgpipgline frontend create %s failed: %s\n", filename, err.Error())
			return 0, err
		}
	}

	frontendWriter, ok := frontendFile.(io.Writer)
	if !ok {
		frontendParam["file"] = frontendFile
		ctx.frontend.funcs["close"](ctx.frontend.handle, frontendParam)
		return 0, errors.New("invalid frontendFile")
	}

	totalSize := hdr.Size
	var wsz int64 = 0

	isTruncated := false
	truncateSize := int64(0)
	for {
		if wsz >= totalSize {
			break
		}

		// get header
		var hbbuf bytes.Buffer
		var header BackupPageHeader
		n, _, err := CopyBufferN(&hbbuf, curInput, buf, hlen, nil)
		if err != nil {
			frontendParam["file"] = frontendFile
			ctx.frontend.funcs["close"](ctx.frontend.handle, frontendParam)
			ctx.logger.Printf("[ERROR] pgpipeline get page header of %s failed: %s\n", filename, err.Error())
			return 0, err
		}
		if n != hlen {
			frontendParam["file"] = frontendFile
			ctx.frontend.funcs["close"](ctx.frontend.handle, frontendParam)
			err = errors.New("copy invalid length of header")
			ctx.logger.Printf("[ERROR] pgpipeline get page header of %s failed: %s\n", filename, err.Error())
			return 0, err
		}
		wsz = wsz + hlen

		binary.Read(&hbbuf, binary.LittleEndian, &header)

		if header.Truncate == TRUNCATE {
			isTruncated = true
			truncateSize = header.Block * DefaultBlockSize
			break
		}

		// seek
		block := header.Block
		offset := block * DefaultBlockSize
		frontendParam["file"] = frontendFile
		frontendParam["offset"] = int(block * DefaultBlockSize)
		_, err = ctx.frontend.funcs["seek"](ctx.frontend.handle, frontendParam)
		if err != nil {
			ctx.logger.Printf("[ERROR] pgpipeline seek %s failed: %s\n", filename, err.Error())
			return 0, err
		}

		// copy one page
		n, _, err = CopyBufferN(frontendWriter, curInput, buf, DefaultBlockSize, ctx.bpsController)
		if err != nil {
			frontendParam["file"] = frontendFile
			ctx.frontend.funcs["close"](ctx.frontend.handle, frontendParam)
			ctx.logger.Printf("[ERROR] pgpipeline extract %s block %d offset %d failed: %s, already writen: %d\n", filename, block, offset, err.Error(), n)
			return 0, err
		}

		if n != DefaultBlockSize {
			frontendParam["file"] = frontendFile

			frontendParam["name"] = filename
			_size, err := ctx.frontend.funcs["size"](ctx.frontend.handle, frontendParam)
			if err != nil {
				ctx.logger.Printf("[ERROR] pgpipeline %s get filesize %s failed: %s\n", ctx.frontend.name, filename, err.Error())
				return 0, err
			}
			size := _size.(int64)

			ctx.frontend.funcs["close"](ctx.frontend.handle, frontendParam)
			err = errors.New("copy invalid length of one block")
			ctx.logger.Printf("[ERROR] pgpipeline extract %s block %d offset %d failed: %s, blocksz:%d, cursize:%d, real copy:%d, curwsz: %d, totalsize:%d, hlen:%d\n", filename, block, offset, err.Error(), header.Block_size, size, n, wsz, totalSize, hlen)
			return 0, err
		}

		// ctx.logger.Printf("!! [INFO] pgpipeline write %s block %d offset %d done\n", filename, block, offset)

		wsz = wsz + n
	}

	if isTruncated {
		frontendParam["name"] = filename
		_size, err := ctx.frontend.funcs["size"](ctx.frontend.handle, frontendParam)
		if err != nil {
			ctx.logger.Printf("[ERROR] pgpipeline %s get filesize %s failed: %s\n", ctx.frontend.name, filename, err.Error())
			return 0, err
		}
		size := _size.(int64)

		if size > truncateSize {
			frontendParam["file"] = frontendFile
			frontendParam["size"] = truncateSize
			_, err := ctx.frontend.funcs["truncate"](ctx.frontend.handle, frontendParam)
			if err != nil {
				ctx.frontend.funcs["close"](ctx.frontend.handle, frontendParam)
				ctx.logger.Printf("[ERROR] pgpipeline %s truncate %s failed: %s\n", ctx.frontend.name, filename, err.Error())
				return 0, err
			}
		}
	}

	if !isTruncated && wsz > totalSize {
		err = errors.New("extract invalid length of block file")
		ctx.logger.Printf("[ERROR] pgpipeline extract %s failed: %s\n", filename, err.Error())
		return 0, err
	}

	frontendParam["file"] = frontendFile
	ctx.frontend.funcs["close"](ctx.frontend.handle, frontendParam)
	if err != nil {
		ctx.logger.Printf("[ERROR] pgpipgline write file %s failed: %s\n", filename, err.Error())
		return 0, err
	}

	return wsz, nil
}

func extractFile(ctx *BackupCtx, name string, input io.Reader, checkCRC bool) error {
	var tarFile *ArchiveFile
	var reader io.Reader
	if ctx.compress {
		if ctx.enableEncryption {
			chachaReader := NewChaCha20Reader(ctx.encryptionPassword, input)
			reader = lz4.NewReader(chachaReader)
		} else {
			reader = lz4.NewReader(input)
		}

	} else {
		if ctx.enableEncryption {
			reader = NewChaCha20Reader(ctx.encryptionPassword, input)
		} else {
			reader = input
		}
	}
	tarFile = NewExtraArchive(name, reader)

	ctx.logger.Printf("[INFO] pgpipeline begin extract tarfile: %s\n", name)
	param := make(map[string]interface{})
	for {
		//fn, size, curInput, err := tarFile.Next()
		hdr, curInput, err := tarFile.Next()
		if err == io.EOF {
			ctx.logger.Printf("[INFO] pgpipeline success extract tarfile: %s\n", name)
			return nil
		}
		if err != nil {
			ctx.logger.Printf("[ERROR] pgpipgline extract %s failed: %s\n", name, err.Error())
			return err
		}
		fn := hdr.Name
		if hdr.Typeflag == tar.TypeDir {
			ctx.logger.Printf("[INFO] pgpipeline extract dir: %s\n", fn)
			param["path"] = fn
			param["mode"] = uint(0700)
			_, err = ctx.frontend.funcs["mkdirs"](ctx.frontend.handle, param)
			if err != nil {
				ctx.logger.Printf("[ERROR] pgpipgline frontend mkdir %s failed: %s\n", fn, err.Error())
				return err
			}
			continue
		}

		dir := path.Dir(fn)
		if dir != "." && dir != ".." {
			param["path"] = dir
			param["mode"] = uint(0700)
			_, err = ctx.frontend.funcs["mkdirs"](ctx.frontend.handle, param)
			if err != nil {
				ctx.logger.Printf("[ERROR] pgpipeline frontend mkdir %s failed: %s\n", dir, err.Error())
				return err
			}
		}

		var wsz int64
		if path.Ext(fn) == DefaultBlockSuffix {
			wsz, err = extractBlockFile(ctx, fn, hdr, curInput, checkCRC)
		} else {
			wsz, err = extractRegularFile(ctx, fn, hdr, curInput, checkCRC)
		}

		if err != nil {
			return err
		}

		err = countBackedSize(ctx, wsz)
		if err != nil {
			return err
		}

		if ctx.stop {
			ctx.logger.Printf("[INFO] receive stop, stop extract tar file: %s now\n", name)
			return nil
		}
	}
}

func _restoreWorkflow(ctx *BackupCtx, checkCRC bool) error {
	ctx.logger.Printf("[INFO] pgpipeline restore begin\n")
	for {
		if ctx.stop {
			ctx.logger.Printf("[INFO] pgpipeline restore work flow stop\n")
			break
		}

		task := ctx.queue.Get()
		if task == nil {
			return nil
		}

		name := task.name
		ctx.logger.Printf("[INFO] pgpipeline start extract %s\n", name)

		var err error
		tryMax := 5
		for i := 0; i < tryMax; i++ {
			err = extractCore(ctx, name, checkCRC)
			if err == nil {
				break
			}
			ctx.logger.Printf("[WARNING] pgpipeline try extract %s %d times failed: %s\n", name, i+1, err.Error())
			if i+1 < tryMax {
				time.Sleep(time.Duration(5*(i+1)) * time.Second)
			}
		}
		if err != nil {
			ctx.logger.Printf("[ERROR] pgpipeline extract %s failed: %s\n", name, err.Error())
			ctx.stop = true
			return err
		}

		ctx.logger.Printf("[INFO] pgpipeline start extract %s success\n", name)
		atomic.AddInt32(&ctx.status.Download, 1)
	}

	if !ctx.stop {
		ctx.logger.Printf("[INFO] workflow restore success\n")
	}

	return nil
}

func extractCore(ctx *BackupCtx, name string, checkCRC bool) error {
	backendParam := make(map[string]interface{})
	backendParam["name"] = name
	file, err := ctx.backend.funcs["open"](ctx.backend.handle, backendParam)
	if err != nil {
		ctx.logger.Printf("[ERROR] pgpipeline open %s failed: %s\n", name, err.Error())
		return err
	}

	backendParam["file"] = file

	reader, ok := file.(io.Reader)
	if !ok {
		ctx.backend.funcs["close"](ctx.backend.handle, backendParam)
		return errors.New("invalid backendFile")
	}

	err = extractFile(ctx, name, reader, checkCRC)
	if err != nil {
		ctx.logger.Printf("[WARNING] pgpipeline extract tar file failed: %s\n", name)
		ctx.backend.funcs["close"](ctx.backend.handle, backendParam)
		return err
	}

	ctx.backend.funcs["close"](ctx.backend.handle, backendParam)
	return nil
}

func backupContent(ctx *BackupCtx, filename, content string) error {
	tarFile, err := newTarFile(ctx)
	if err != nil {
		ctx.logger.Printf("[ERROR] pgpipeline create tar stream failed: %s\n", err.Error())
		return err
	}
	ctx.logger.Printf("[INFO] pgpipeline create new tar [%s]\n", tarFile.name)
	var archiveErr, uploadErr error
	var count int

	go func() {
		defer func() {
			tarFile.tw.Close()
			tarFile.w.Close()
		}()
		archiveErr = tarFile.AddMeta("backup_label", int64(len(content)))
		if archiveErr != nil {
			ctx.logger.Printf("[ERROR] pgpipeline add backup_label meta failed: %s\n", archiveErr.Error())
			return
		}
		_, archiveErr = tarFile.tw.Write([]byte(content))
		if archiveErr != nil {
			ctx.logger.Printf("[ERROR] pgpipeline add backup_label to tar stream failed: %s\n", archiveErr.Error())
			return
		}
		count, archiveErr = archiveFiles(ctx, false, NOTSETCRC, DISABLEMAXFILE, tarFile)
		if archiveErr != nil {
			ctx.logger.Printf("[ERROR] pgpipeline add global/pg_control to tar stream failed: %s\n", archiveErr.Error())
			return
		}
	}()

	var wait sync.WaitGroup
	wait.Add(1)
	go func() {
		uploadErr = uploadArchiveFile(ctx, tarFile)
		wait.Done()
	}()

	wait.Wait()
	if uploadErr != nil {
		ctx.logger.Printf("[ERROR] pgpipeline upload failed: %s\n", uploadErr.Error())
		err = uploadErr
	}
	if archiveErr != nil {
		ctx.logger.Printf("[ERROR] pgpipeline archiveErr failed: %s\n", archiveErr.Error())
		err = archiveErr
	}
	if uploadErr == nil && archiveErr == nil {
		ctx.logger.Printf("[INFO] pgpipeline upload files: %d\n", count)
		return nil
	}
	return err
}

func getFileContent(ctx *BackupCtx, filename string) ([]byte, error) {
	frontendParam := make(map[string]interface{})
	var buf bytes.Buffer

	frontendParam["name"] = filename
	frontendParam["flags"] = uint(00)
	frontendParam["mode"] = uint(0400)
	frontendFile, err := ctx.frontend.funcs["open"](ctx.frontend.handle, frontendParam)
	if err != nil {
		ctx.logger.Printf("[ERROR] pgpipeline %s open %s failed: %s\n", ctx.frontend.name, filename, err.Error())
		return nil, err
	}

	frontendParam["file"] = frontendFile

	frontendReader, ok := frontendFile.(io.Reader)
	if !ok {
		ctx.frontend.funcs["close"](ctx.frontend.handle, frontendParam)
		return nil, errors.New("invalid frontendFile")
	}

	_, _, err = Copy(&buf, frontendReader)
	if err != nil {
		ctx.logger.Printf("[ERROR] pgpipeline normal mode, copy file failed: %s\n", err.Error())
		ctx.frontend.funcs["close"](ctx.frontend.handle, frontendParam)
		return nil, err
	}

	ctx.frontend.funcs["close"](ctx.frontend.handle, frontendParam)

	return buf.Bytes(), nil
}

func localFilesTarFile(ctx *BackupCtx, filename string) (*ArchiveFile, error) {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()

	var tarName string
	if ctx.compress {
		tarName = filename + ".tar.lz4"
	} else {
		tarName = filename + ".tar"
	}
	tarFile, err := NewArchive(tarName)
	if err != nil {
		return nil, err
	}
	ctx.localTarFile = tarName
	return tarFile, nil
}

func newTarFile(ctx *BackupCtx) (*ArchiveFile, error) {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()

	tarName := getTarName(ctx)
	tarFile, err := NewArchive(tarName)
	if err != nil {
		return nil, err
	}
	ctx.tarFiles = append(ctx.tarFiles, tarName)
	return tarFile, nil
}

func paddingZeroToStream(writer io.Writer, size int64) error {
	buf := make([]byte, 16*1024)
	l := int64(len(buf))
	if l > size {
		_, err := writer.Write(buf[0:size])
		return err
	}

	count := size / l
	left := size % l
	for count > 0 {
		_, err := writer.Write(buf)
		if err != nil {
			return err
		}
		count--
	}
	if left > 0 {
		_, err := writer.Write(buf[0:left])
		return err
	}
	return nil
}

func archiveFile(ctx *BackupCtx, file string, tarFile *ArchiveFile, setCRC bool) (int, error) {
	buf := make([]byte, FileBufSize)

	frontendParam := make(map[string]interface{})

	frontendParam["path"] = file
	_isdir, err := ctx.frontend.funcs["isdir"](ctx.frontend.handle, frontendParam)
	if err != nil {
		if IsNotExist(err) {
			ctx.logger.Printf("[WARNING] pgpipeline %s get file info(dir) %s failed: %s\n", ctx.frontend.name, file, err.Error())
			return 0, nil
		}
		ctx.logger.Printf("[ERROR] pgpipeline %s get file info(dir) %s failed: %s\n", ctx.frontend.name, file, err.Error())
		return 0, err
	}
	isdir := _isdir.(bool)
	if isdir {
		err = tarFile.AddDir(file)
		if err != nil {
			ctx.logger.Printf("[ERROR] pgpipeline %s add dir %s failed: %s\n", ctx.frontend.name, file, err.Error())
			return 0, err
		}
		return 0, nil
	}

	// get file size
	frontendParam["name"] = file
	_size, err := ctx.frontend.funcs["size"](ctx.frontend.handle, frontendParam)
	if err != nil {
		if IsNotExist(err) {
			ctx.logger.Printf("[INFO] pgpipeline %s get filesize %s failed: %s\n", ctx.frontend.name, file, err.Error())
			return 0, nil
		}
		ctx.logger.Printf("[ERROR] pgpipeline %s get filesize %s failed: %s\n", ctx.frontend.name, file, err.Error())
		return 0, err
	}
	size := _size.(int64)
	ctx.logger.Printf("[INFO] pgpipeline start archive %s to %s(%d) ...\n", file, tarFile.name, size)

	// get file handle
	frontendParam["name"] = file
	frontendParam["flags"] = uint(00)
	frontendParam["mode"] = uint(0400)
	frontendFile, err := ctx.frontend.funcs["open"](ctx.frontend.handle, frontendParam)
	if err != nil {
		if IsNotExist(err) {
			ctx.logger.Printf("[INFO] pgpipeline %s open %s failed: %s\n", ctx.frontend.name, file, err.Error())
			return 0, nil
		}
		ctx.logger.Printf("[ERROR] pgpipeline %s open %s failed: %s\n", ctx.frontend.name, file, err.Error())
		return 0, err
	}
	frontendReader, ok := frontendFile.(io.Reader)
	if !ok {
		frontendParam["file"] = frontendFile
		ctx.frontend.funcs["close"](ctx.frontend.handle, frontendParam)
		return 0, errors.New("invalid frontendFile")
	}

	// add tar filemeta
	ctx.logger.Printf("[INFO] pgpipeline add %s meta to %s ...\n", file, tarFile.name)
	name := strings.TrimLeft(file, "/")
	err = tarFile.AddMeta(name, size)
	ctx.logger.Printf("[INFO] pgpipeline copying %s meta to %s ...\n", file, tarFile.name)

	// copy file content to tar
	n, crc, err := CopyBufferN(tarFile.tw, frontendReader, buf, size, ctx.bpsController)
	if err != nil {
		frontendParam["file"] = frontendFile
		ctx.frontend.funcs["close"](ctx.frontend.handle, frontendParam)
		ctx.logger.Printf("[ERROR] pgpipeline archive %s failed: %s\n", file, err.Error())
		return 0, err
	} else {
		// padding zero for truncate file
		if n < size {
			left := size - n
			err = paddingZeroToStream(tarFile.tw, left)
			if err != nil {
				frontendParam["file"] = frontendFile
				ctx.frontend.funcs["close"](ctx.frontend.handle, frontendParam)
				ctx.logger.Printf("[ERROR] pgpipeline archive %s failed: %s\n", file, err.Error())
				return 0, err
			}
			ctx.logger.Printf("[INFO] padding zero for %s\n", file)
		}
	}

	if setCRC {
		_crc, ok := ctx.crcMap.Load(file)
		if ok && _crc != crc {
			ctx.logger.Printf("[ERROR] set crc %d of file %s before, now %d", _crc, file, crc)
			return 0, errors.New("multi crc of file")
		}
		ctx.crcMap.Store(file, crc)
	}

	// close file
	frontendParam["file"] = frontendFile
	ctx.frontend.funcs["close"](ctx.frontend.handle, frontendParam)

	return int(n), nil
}

func formatFile(file string) (string, error) {
	if len(file) < 15 {
		return "", errors.New("invalid file to format, too short")
	}

	_newfile := []byte(file[14:])
	for i := 0; i < len(_newfile); i++ {
		if _newfile[i] == '-' {
			_newfile[i] = '/'
		}
	}
	return string(_newfile), nil
}

func archiveBlock(ctx *BackupCtx, file string, tarFile *ArchiveFile, setCRC bool) (int, error) {
	buf := make([]byte, FileBufSize)

	// build page data map
	var page DataPageMap

	var bbuf bytes.Buffer

	param := make(map[string]interface{})
	param["name"] = file
	blockFile, err := HTTPOpen(ctx.blockCtx, param)
	if err != nil {
		ctx.logger.Printf("[ERROR] sftp client open remote file %s failed: %s\n", file, err.Error())
		return 0, err
	}
	blockFileReader, ok := blockFile.(io.Reader)
	if !ok {
		return 0, errors.New("invalid blockFile")
	}
	param["file"] = blockFile
	defer HTTPClose(ctx.blockCtx, param)

	_, _, err = Copy(&bbuf, blockFileReader)
	if err != nil {
		ctx.logger.Printf("[ERROR] pgpipeline normal mode, copy file failed: %s\n", err.Error())
		return 0, err
	}

	page.init(bbuf.Bytes())

	frontendParam := make(map[string]interface{})

	file, err = formatFile(file)
	if err != nil {
		ctx.logger.Printf("[ERROR] pgpipeline normal mode, format file failed: %s\n", err.Error())
		return 0, err
	}

	frontendParam["path"] = file

	// get file size
	hlen := int(unsafe.Sizeof(BackupPageHeader{}))
	size := (hlen + DefaultBlockSize) * page.blockcount

	// get file handle
	frontendParam["name"] = file
	frontendParam["flags"] = uint(00)
	frontendParam["mode"] = uint(0400)
	frontendFile, err := ctx.frontend.funcs["open"](ctx.frontend.handle, frontendParam)
	if err != nil {
		if IsNotExist(err) {
			ctx.logger.Printf("[INFO] pgpipeline %s open %s failed: %s\n", ctx.frontend.name, file, err.Error())
			return 0, nil
		}
		ctx.logger.Printf("[ERROR] pgpipeline %s open %s failed: %s\n", ctx.frontend.name, file, err.Error())
		return 0, err
	}
	frontendReader, ok := frontendFile.(io.Reader)
	if !ok {
		frontendParam["file"] = frontendFile
		ctx.frontend.funcs["close"](ctx.frontend.handle, frontendParam)
		return 0, errors.New("invalid frontendFile")
	}

	// add tar filemeta
	ctx.logger.Printf("[INFO] pgpipeline add %s meta to %s ...\n", file, tarFile.name)
	name := strings.TrimLeft(file, "/") + DefaultBlockSuffix
	err = tarFile.AddMeta(name, int64(size))
	ctx.logger.Printf("[INFO] pgpipeline %s tar size %d\n", name, size)
	ctx.logger.Printf("[INFO] pgpipeline copying %s meta to %s ...\n", name, tarFile.name)

	// iterate page copy
	ctx.logger.Printf("[INFO] page count of %s is %d\n", file, page.blockcount)
	isTruncated := false
	var writeSz int64 = 0
	for {
		ok, block := page.datapagemap_next()
		if !ok {
			break
		}

		var blockbuf bytes.Buffer

		frontendParam["file"] = frontendFile
		frontendParam["offset"] = block * DefaultBlockSize
		offset, err := ctx.frontend.funcs["seek"](ctx.frontend.handle, frontendParam)
		if err != nil {
			ctx.logger.Printf("[ERROR] pgpipeline seek %s failed: %s\n", file, err.Error())
			return 0, err
		}
		if offset != frontendParam["offset"] {
			err = errors.New("offset return by seek not valid")
			ctx.logger.Printf("[ERROR] pgpipeline seek %s failed: %s, offset set: %d, return: %d\n", file, err.Error(), frontendParam["offset"], offset)
			return 0, err
		}

		n, _, err := CopyBufferN(&blockbuf, frontendReader, buf, DefaultBlockSize, ctx.bpsController)
		if err != nil {
			frontendParam["file"] = frontendFile
			ctx.frontend.funcs["close"](ctx.frontend.handle, frontendParam)
			ctx.logger.Printf("[ERROR] pgpipeline archive %s block %d offset %d failed: %s\n", file, block, offset, err.Error())
			return 0, err
		}

		if n == 0 {
			isTruncated = true
			ctx.logger.Printf("[INFO] file %s truncate at block %d\n", file, block)
		} else if n != DefaultBlockSize {
			frontendParam["file"] = frontendFile
			ctx.frontend.funcs["close"](ctx.frontend.handle, frontendParam)
			err = errors.New("copy invalid length of one block")
			ctx.logger.Printf("[ERROR] pgpipeline archive %s block %d offset %d failed: %s\n", file, block, offset, err.Error())
			return 0, err
		}

		var hbbuf bytes.Buffer
		var header BackupPageHeader
		header.Block = int64(block)
		header.Block_size = 8192
		if isTruncated {
			header.Truncate = TRUNCATE
		} else {
			header.Truncate = NOTTRUNCATE
		}

		err = binary.Write(&hbbuf, binary.LittleEndian, header)
		if err != nil {
			ctx.logger.Printf("[ERROR] encode page header failed: %s\n", err.Error())
			return 0, errors.New("encode page header failed:" + err.Error())
		}

		cn, _, err := Copy(tarFile.tw, &hbbuf)
		if err != nil {
			ctx.logger.Printf("[ERROR] pgpipeline normal mode, copy file failed: %s\n", err.Error())
			return 0, err
		}

		if int(cn) != hlen {
			err = errors.New("copy invalid len")
			ctx.logger.Printf("[ERROR] pgpipeline normal mode, copy header failed, invalid len %d\n", cn)
			return 0, err
		}

		writeSz = writeSz + cn

		if isTruncated {
			break
		}

		n, _, err = Copy(tarFile.tw, &blockbuf)
		if err != nil {
			ctx.logger.Printf("[ERROR] pgpipeline normal mode, copy file failed: %s\n", err.Error())
			return 0, err
		}

		if n != DefaultBlockSize {
			frontendParam["file"] = frontendFile
			ctx.frontend.funcs["close"](ctx.frontend.handle, frontendParam)
			err = errors.New("copy invalid length of one block")
			ctx.logger.Printf("[ERROR] pgpipeline archive %s block %d offset %d failed: %s\n", file, block, offset, err.Error())
			return 0, err
		}

		writeSz = writeSz + n
	}

	if isTruncated {
		var left int64
		myDiscard := MyDiscard{}

		left = int64(size) - writeSz
		if left > 0 {
			n, _, err := CopyBufferN(tarFile.tw, &myDiscard, buf, left, ctx.bpsController)
			if err != nil {
				frontendParam["file"] = frontendFile
				ctx.frontend.funcs["close"](ctx.frontend.handle, frontendParam)
				ctx.logger.Printf("[ERROR] pgpipeline archive %s size: %d as truncated failed: %s\n", file, left, err.Error())
				return 0, err
			}

			if n != left {
				err = errors.New("copy invalid len")
				frontendParam["file"] = frontendFile
				ctx.frontend.funcs["close"](ctx.frontend.handle, frontendParam)
				ctx.logger.Printf("[ERROR] pgpipeline archive %s size: %d as truncated failed: %s\n", file, left, err.Error())
				return 0, err
			}
		}
	}

	// close file
	frontendParam["file"] = frontendFile
	ctx.frontend.funcs["close"](ctx.frontend.handle, frontendParam)

	return size, nil
}

func archiveFiles(ctx *BackupCtx, useBlock bool, setCRC bool, enableMaxfile bool, tarFile *ArchiveFile) (int, error) {
	count := 0
	done := false
	var n, totalSize int
	var err error
	for !done {
		if ctx.stop {
			done = true
		}

		task := ctx.queue.Get()
		if task == nil {
			done = true
			break
		}

		if useBlock {
			n, err = archiveBlock(ctx, task.name, tarFile, setCRC)
			if err != nil {
				return count, err
			}
		} else {
			n, err = archiveFile(ctx, task.name, tarFile, setCRC)
			if err != nil {
				return count, err
			}
		}

		count++
		totalSize += n
		if enableMaxfile && int64(totalSize) >= ctx.conf.MaxFileSize {
			break
		}
	}
	return count, nil
}

func uploadArchiveFile(ctx *BackupCtx, tarFile *ArchiveFile) error {
	buf := make([]byte, FileBufSize)
	defer tarFile.r.Close()

	backendParam := make(map[string]interface{})
	// open backend file
	backendParam["name"] = tarFile.name
	backendFile, err := ctx.backend.funcs["create"](ctx.backend.handle, backendParam)
	if err != nil {
		ctx.logger.Printf("[ERROR] pgpipeline %s create file %s failed: %s\n", ctx.backend.name, tarFile.name, err.Error())
		return err
	}

	backendWriter, ok := backendFile.(io.Writer)
	if !ok {
		backendParam["file"] = backendFile
		ctx.backend.funcs["close"](ctx.backend.handle, backendParam)
		return errors.New("invalid backendFile")
	}

	if ctx.compress {
		if ctx.enableEncryption {
			chachaWriter := NewChaCha20Writer(ctx.encryptionPassword, backendWriter)
			writer := lz4.NewWriter(chachaWriter)
			_, _, err = CopyBuffer(writer, tarFile.r, buf, nil)
			writer.Close()
		} else {
			writer := lz4.NewWriter(backendWriter)
			_, _, err = CopyBuffer(writer, tarFile.r, buf, nil)
			writer.Close()
		}
	} else {
		if ctx.enableEncryption {
			chachaWriter := NewChaCha20Writer(ctx.encryptionPassword, backendWriter)
			_, _, err = CopyBuffer(chachaWriter, tarFile.r, buf, nil)
		} else {
			_, _, err = CopyBuffer(backendWriter, tarFile.r, buf, nil)
		}
	}

	if err != nil {
		ctx.logger.Printf("[ERROR] pgpipeline upload file %s failed: %s\n", tarFile.name, err.Error())
	} else {
		ctx.logger.Printf("[INFO] pgpipeline upload file %s success\n", tarFile.name)
	}
	backendParam["file"] = backendFile
	ctx.backend.funcs["close"](ctx.backend.handle, backendParam)

	return err
}

func backupWorkflow(ctx *BackupCtx) error {
	err := _backupWorkflow(ctx, false, SETCRC, ENABLEMAXFILE, "")
	return err
}

// raise a msg when backup file failed
func _backupWorkflow(ctx *BackupCtx, useBlock bool, setCRC bool, enableMaxfile bool, filename string) error {
	var err error
	var tarFile *ArchiveFile
	stop := false
	for {
		if ctx.queue.Length() == 0 {
			break
		}

		if ctx.stop {
			ctx.logger.Printf("[INFO] pgpipeline stop backup work flow\n")
			break
		}

		if filename == "" {
			tarFile, err = newTarFile(ctx)
		} else {
			tarFile, err = localFilesTarFile(ctx, filename)
		}
		if err != nil {
			ctx.logger.Printf("[ERROR] pgpipeline create tar stream failed: %s\n", err.Error())
			return err
		}
		ctx.logger.Printf("[INFO] pgpipeline create new tar [%s]\n", tarFile.name)

		var archiveErr, uploadErr error
		var count int

		var wait sync.WaitGroup

		wait.Add(1)
		go func() {
			ctx.logger.Printf("[INFO] pgpipeline start create tar file: %s\n", tarFile.name)
			count, archiveErr = archiveFiles(ctx, useBlock, setCRC, enableMaxfile, tarFile)
			if archiveErr != nil {
				ctx.logger.Printf("[ERROR] pgpipeline add file to tar stream failed: %s\n", archiveErr.Error())
			}
			tarFile.tw.Close()
			tarFile.w.Close()
			ctx.logger.Printf("[INFO] pgpipeline create tar file: %s success\n", tarFile.name)
			if count == 0 && archiveErr == nil {
				// no task found, just stop it
				stop = true
			}
			wait.Done()
		}()

		wait.Add(1)
		go func() {
			ctx.logger.Printf("[INFO] pgpipeline write to backend begin: %s\n", tarFile.name)
			uploadErr = uploadArchiveFile(ctx, tarFile)
			ctx.logger.Printf("[INFO] pgpipeline write to backend success: %s\n", tarFile.name)
			wait.Done()
		}()

		wait.Wait()

		if uploadErr != nil {
			ctx.logger.Printf("[ERROR] pgpipeline upload failed: %s\n", uploadErr.Error())
			err = uploadErr
		}
		if archiveErr != nil {
			ctx.logger.Printf("[ERROR] pgpipeline archiveErr failed: %s\n", archiveErr.Error())
			err = archiveErr
		}
		if uploadErr == nil && archiveErr == nil {
			ctx.logger.Printf("[INFO] pgpipeline upload files: %d\n", count)
		}

		if stop {
			// close tarfile, donot upload to remote
			tarFile.tw.Close()
			tarFile.w.Close()
			tarFile.r.Close()
			break
		}
	}
	return err
}

func waitUntilStop(backupCtx *BackupCtx, done chan struct{}) {
	stop := false
	for !stop {
		select {
		case <-done:
			stop = true
			break
		case <-backupCtx.stopEvent:
			stop = true
			backupCtx.stop = true
			break
		case <-time.After(1 * time.Second):
			break
		}
	}
}

func getMetaName(ctx *BackupCtx) string {
	return strings.Join([]string{ctx.id, META_EXT}, ".")
}

func backupMeta(ctx *BackupCtx, params map[string]interface{}, meta []byte) error {
	filename := getMetaName(ctx)
	params["name"] = filename
	backendFile, err := ctx.backend.funcs["create"](ctx.backend.handle, params)
	if err != nil {
		ctx.logger.Printf("[ERROR] pgpipeline create backup meta %s failed: %s\n", filename, err.Error())
		return err
	}
	params["file"] = backendFile
	params["buf"] = meta
	_, err = ctx.backend.funcs["write"](ctx.backend.handle, params)
	if err != nil {
		ctx.logger.Printf("[ERROR] pgpipeline write backup meta %s failed: %s\n", filename, err.Error())
		ctx.backend.funcs["close"](ctx.backend.handle, params)
		return err
	}
	ctx.backend.funcs["close"](ctx.backend.handle, params)
	return nil
}

func backupMetaToLocal(ctx *BackupCtx, meta []byte) error {
	filename := getMetaName(ctx)
	fullname := ctx.conf.BackupMetaDir + "/" + filename

	f, err := os.Create(fullname)
	defer f.Close()
	if err != nil {
		return err
	}
	_, err = f.Write(meta)
	return err
}

func backupDma(backupCtx *BackupCtx, params map[string]interface{}) error {
	backupCtx.logger.Printf("[INFO] pgpipeline backup dma\n")

	params["path"] = "polar_dma"
	params["mode"] = F_OK
	ret, err := backupCtx.frontend.funcs["access"](backupCtx.frontend.handle, params)
	if err != nil {
		backupCtx.logger.Printf("[ERROR] pgpipeline access %s failed: %s\n", params["path"], err.Error())
		return err
	}

	if ret == 0 {
		backupCtx.queue.AddTask(NewTask("polar_dma"))
		err := countBackupSize(backupCtx, "polar_dma")
		if err != nil {
			return err
		}
		err = backupWorkflow(backupCtx)
		if err != nil {
			return err
		}
		backupCtx.pgFiles = append(backupCtx.pgFiles, PGFile{"polar_dma", 0})

		params["path"] = "polar_dma/consensus_meta"
		params["mode"] = F_OK
		ret, err = backupCtx.frontend.funcs["access"](backupCtx.frontend.handle, params)
		if err != nil {
			backupCtx.logger.Printf("[ERROR] pgpipeline access %s failed: %s\n", params["path"], err.Error())
			return err
		}
		if ret == 0 {
			backupCtx.queue.AddTask(NewTask("polar_dma/consensus_meta"))
			err := countBackupSize(backupCtx, "polar_dma/consensus_meta")
			if err != nil {
				return err
			}
			err = backupWorkflow(backupCtx)
			if err != nil {
				return err
			}
			backupCtx.logger.Printf("[INFO] pgpipeline backup polar_dma/consensus_meta done\n")
		}
		backupCtx.pgFiles = append(backupCtx.pgFiles, PGFile{"polar_dma/consensus_meta", 0})

		params["path"] = "polar_dma/consensus_log"
		params["mode"] = F_OK
		ret, err = backupCtx.frontend.funcs["access"](backupCtx.frontend.handle, params)
		if err != nil {
			backupCtx.logger.Printf("[ERROR] pgpipeline access %s failed: %s\n", params["path"], err.Error())
			return err
		}
		if ret == 0 {
			err := backupFolder(backupCtx, params, "polar_dma/consensus_log")
			if err != nil {
				backupCtx.logger.Printf("[ERROR] pgpipeline backup %s failed: %s\n", params["path"], err.Error())
				return err
			}
		}
		backupCtx.pgFiles = append(backupCtx.pgFiles, PGFile{"polar_dma/consensus_log", 0})

		params["path"] = "polar_dma/consensus_cc_log"
		params["mode"] = F_OK
		ret, err = backupCtx.frontend.funcs["access"](backupCtx.frontend.handle, params)
		if err != nil {
			backupCtx.logger.Printf("[ERROR] pgpipeline access %s failed: %s\n", params["path"], err.Error())
			return err
		}
		if ret == 0 {
			err := backupFolder(backupCtx, params, "polar_dma/consensus_cc_log")
			if err != nil {
				backupCtx.logger.Printf("[ERROR] pgpipeline backup %s failed: %s\n", params["path"], err.Error())
				return err
			}
		}
		backupCtx.pgFiles = append(backupCtx.pgFiles, PGFile{"polar_dma/consensus_cc_log", 0})
	}

	backupCtx.logger.Printf("[INFO] pgpipeline backup dma done\n")
	return nil
}

func backupFolder(backupCtx *BackupCtx, params map[string]interface{}, folder string) error {
	files, err := listdirRecursion(folder, params, &backupCtx.frontend)
	if err != nil {
		return err
	}
	for _, file := range files {
		backupCtx.queue.AddTask(NewTask(file))
		err := countBackupSize(backupCtx, file)
		if err != nil {
			return err
		}
		backupCtx.pgFiles = append(backupCtx.pgFiles, PGFile{file, 0})
	}

	var wait sync.WaitGroup
	backupDone := make(chan struct{})
	parallelBackupWorkflow(backupCtx, false, backupCtx.workerCount, &wait)
	go func() {
		wait.Wait()
		close(backupDone)
	}()
	waitUntilStop(backupCtx, backupDone)
	if backupCtx.fail > 0 {
		backupCtx.logger.Printf("[ERROR] pgpipeline backup folder %s failed\n", folder)
		return errors.New("backup folder " + folder + " failed")
	}
	backupCtx.logger.Printf("[INFO] pgpipeline backup folder %s done\n", folder)
	return nil
}

func backupLocalFiles(backupCtx *BackupCtx, params map[string]interface{}, folder string, name string) error {
	if backupCtx.localFile.handle == nil {
		err := errors.New("backupCtx.localFile.handle is nil. Please check the config.")
		backupCtx.logger.Printf("[ERROR] pgpipeline backup local files %s failed: %s\n", folder, err.Error())
		return err
	}

	frontend := backupCtx.frontend
	backupCtx.frontend = backupCtx.localFile
	defer func() {
		backupCtx.frontend = frontend
	}()

	folder = ""
	files, err := listdirRecursion(folder, params, &backupCtx.frontend)
	if err != nil {
		return err
	}

	for _, file := range files {
		if filepath.Ext(file) == ".log" {
			continue
		}
		backupCtx.queue.AddTask(NewTask(file))
	}

	err = _backupWorkflow(backupCtx, false, NOTSETCRC, DISABLEMAXFILE, name)
	if err != nil {
		backupCtx.logger.Printf("[ERROR] pgpipeline backup local files %s failed: %s\n", folder, err.Error())
		return err
	}
	backupCtx.logger.Printf("[INFO] pgpipeline backup local files %s done\n", folder)
	return nil
}

func backupLog(backupCtx *BackupCtx, params map[string]interface{}, startWalName string) error {
	backupCtx.logger.Printf("[INFO] pgpipeline backup log\n")

	walExsample := "pg_wal/000000010000005200000003"
	readyExsample := "pg_wal/archive_status/000000010000005200000003.ready"
	walLen := len(walExsample)
	readyLen := len(readyExsample)

	startWalReadyName := startWalName + ".ready"

	set := make(map[string]struct{})
	for _, file := range backupCtx.pgFiles {
		set[file.Name] = struct{}{}
	}

	files, err := listdirRecursion(backupCtx.logdir, params, &backupCtx.frontend)
	if err != nil {
		return err
	}
	for _, file := range files {
		if len(file) == walLen {
			strArray := strings.Split(file, "/")
			walName := strArray[len(strArray)-1]
			if walName < startWalName {
				continue
			}
		} else if len(file) == readyLen {
			strArray := strings.Split(file, "/")
			walReadyName := strArray[len(strArray)-1]
			if walReadyName < startWalReadyName {
				continue
			}
		}

		backupCtx.logger.Printf("[INFO] agent will backup log file: %s\n", file)
		if _, ok := set[file]; !ok {
			backupCtx.pgFiles = append(backupCtx.pgFiles, PGFile{file, 0})
		}
		backupCtx.queue.AddTask(NewTask(file))
		err := countBackupSize(backupCtx, file)
		if err != nil {
			return err
		}
	}

	backupCtx.logger.Printf("[INFO] pgpipeline estimate backup size : %d, real backup size : %d\n", backupCtx.status.BackupSize, backupCtx.status.RealbkSize)
	// after stat size of log files, we obtain the final real backup size, therefore, instead estimate backup size of real backup size
	backupCtx.status.BackupSize = backupCtx.status.RealbkSize

	var wait sync.WaitGroup
	backupLogdone := make(chan struct{})
	parallelBackupWorkflow(backupCtx, false, backupCtx.workerCount, &wait)
	go func() {
		wait.Wait()
		close(backupLogdone)
	}()
	waitUntilStop(backupCtx, backupLogdone)
	if backupCtx.fail > 0 {
		backupCtx.logger.Printf("[ERROR] pgpipeline backup log failed\n")
		return errors.New("backup log failed")
	}
	backupCtx.logger.Printf("[INFO] pgpipeline backup log done\n")
	return nil
}

func estimateBackupSize(ctx *BackupCtx) error {
	ctx.logger.Printf("[INFO] pgpipeline %s estimate backup size\n", ctx.frontend.name)
	param := make(map[string]interface{})
	param["path"] = ""
	_size, err := ctx.frontend.funcs["du"](ctx.frontend.handle, param)
	if err != nil {
		ctx.logger.Printf("[ERROR] pgpipeline %s estimate backup size failed: %s\n", ctx.frontend.name, err.Error())
		return err
	}
	size := _size.(int64)
	ctx.status.BackupSize = size
	ctx.logger.Printf("[INFO] pgpipeline %s estimate backup size: %d\n", ctx.frontend.name, ctx.status.BackupSize)
	return nil
}

func countBackupSize(ctx *BackupCtx, file string) error {
	param := make(map[string]interface{})
	param["name"] = file
	_size, err := ctx.frontend.funcs["size"](ctx.frontend.handle, param)
	if err != nil {
		if IsNotExist(err) {
			ctx.logger.Printf("[INFO] pgpipeline %s get filesize %s failed: %s\n", ctx.frontend.name, file, err.Error())
			return nil
		}
		ctx.logger.Printf("[ERROR] pgpipeline %s get filesize %s failed: %s\n", ctx.frontend.name, file, err.Error())
		return err
	}
	size := _size.(int64)
	atomic.AddInt64(&ctx.status.RealbkSize, size)
	return nil
}

func countBackedSize(backupCtx *BackupCtx, filesz int64) error {
	atomic.AddInt64(&backupCtx.status.BackedSize, filesz)
	return nil
}

func updateBackedSize(backupCtx *BackupCtx, filesz int64) error {
	backupCtx.status.BackedSize = filesz
	return nil
}

func backupData(backupCtx *BackupCtx, dirs []string, params map[string]interface{}) error {
	backupCtx.logger.Printf("[INFO] pgpipeline backup data\n")

	var err error
	backupCtx.logger.Printf("[INFO] pgpipeline backup full data\n")
	err = backupFullData(backupCtx, dirs, params)
	if err != nil {
		return err
	}

	if backupCtx.useBlock {
		backupCtx.logger.Printf("[INFO] pgpipeline backup block data\n")
		err = backupBlockData(backupCtx, dirs, params)
	}

	return err
}

func backupFullData(backupCtx *BackupCtx, files []string, params map[string]interface{}) error {
	param := make(map[string]interface{})
	var dirs []string
	for _, file := range files {
		param["path"] = file
		isexist, err := backupCtx.frontend.funcs["exist"](backupCtx.frontend.handle, param)
		if err != nil {
			backupCtx.logger.Printf("[ERROR] pgpipeline exist %s failed: %s\n", param["path"], err.Error())
			return err
		}
		if !isexist.(bool) {
			continue
		}
		status, err := backupCtx.frontend.funcs["isdir"](backupCtx.frontend.handle, param)
		if err != nil {
			return err
		}
		isNeedBackup := false
		if status.(bool) {
			if needBackup(file, backupCtx.excludeDirs) {
				isNeedBackup = true
				dirs = append(dirs, file)
			}
		} else {
			if needBackup(file, backupCtx.excludeFiles) {
				isNeedBackup = true
				backupCtx.pgFiles = append(backupCtx.pgFiles, PGFile{file, 0})
			}
		}
		if isNeedBackup {
			backupCtx.queue.AddTask(NewTask(file))
			err := countBackupSize(backupCtx, file)
			if err != nil {
				return err
			}
		}
	}
	bkfiles, err := getBackupFiles(backupCtx, dirs, params)
	if err != nil {
		backupCtx.logger.Printf("[ERROR] pgpipeline frontend readdir: %s, failed: %s\n", strings.Join(dirs, ","), err.Error())
		return err
	}

	for _, file := range bkfiles {
		// skip pg_control
		if file == "global/pg_control" {
			continue
		}

		if backupCtx.useBlock {
			if checkRelFile(file) {
				continue
			}
		}

		backupCtx.queue.AddTask(NewTask(file))
		err := countBackupSize(backupCtx, file)
		if err != nil {
			return err
		}
	}

	backupCtx.logger.Printf("[INFO] pgpipeline get all full backup data files\n")
	backupCtx.logger.Printf("[INFO] pgpipeline worker count: %d\n", backupCtx.workerCount)
	workerCount := backupCtx.workerCount
	var wait sync.WaitGroup
	parallelBackupWorkflow(backupCtx, false, workerCount, &wait)

	backupDatadone := make(chan struct{})
	go func() {
		wait.Wait()
		close(backupDatadone)
	}()

	waitUntilStop(backupCtx, backupDatadone)
	backupCtx.logger.Printf("[INFO] pgpipeline backup data done\n")
	if backupCtx.fail > 0 {
		return errors.New("backup data failed")
	}
	return nil
}

func backupBlockData(backupCtx *BackupCtx, dirs []string, params map[string]interface{}) error {
	// sshcnn, err := createSSHConnect(backupCtx.incrementalip)
	// if err != nil {
	//     backupCtx.logger.Printf("[ERROR] create ssh connect failed: %s\n", err.Error())
	//     return err
	// }
	// defer sshcnn.Close()

	// sftpcli, err := sftp.NewClient(sshcnn)
	// if err != nil {
	// 	backupCtx.logger.Printf("[ERROR] create sftp client failed: %s\n", err.Error())
	//     return err
	// }
	// defer sftpcli.Close()
	param := make(map[string]interface{})
	_files, err := HTTPReaddir(backupCtx.blockCtx, param)
	if err != nil {
		backupCtx.logger.Printf("[ERROR] http client read block dir failed: %s\n", err.Error())
		return err
	}

	files, ok := _files.([]string)
	if !ok {
		return errors.New("HTTPReaddir should return []string")
	}

	for _, file := range files {
		if path.Ext(file) == ".lock" {
			continue
		}
		backupCtx.logger.Printf("[INFO] add block index file: %s\n", file)
		backupCtx.queue.AddTask(NewTask(file))
	}

	// file, err := os.Open(backupCtx.blockBackupDir)
	// if err != nil {
	//     return err
	// }
	// defer file.Close()

	// files, err := file.Readdirnames(-1)
	// if err != nil {
	//     backupCtx.logger.Printf("[ERROR] read block files failed: %s\n", err.Error())
	//     return err
	// }

	// for _, file := range(files) {
	//     if path.Ext(file) == ".lock" {
	//         continue
	//     }
	//     backupCtx.queue.AddTask(NewTask(file))
	//     // TODO count backup size from bitmap
	//     // err := countBackupSize(backupCtx, file)
	//     // if err != nil {
	//     //     return err
	//     // }
	// }

	backupCtx.logger.Printf("[INFO] pgpipeline get all block backup data files\n")
	backupCtx.logger.Printf("[INFO] pgpipeline worker count: %d\n", backupCtx.workerCount)
	workerCount := backupCtx.workerCount
	var wait sync.WaitGroup
	parallelBackupWorkflow(backupCtx, backupCtx.useBlock, workerCount, &wait)

	backupDatadone := make(chan struct{})
	go func() {
		wait.Wait()
		close(backupDatadone)
	}()

	waitUntilStop(backupCtx, backupDatadone)
	backupCtx.logger.Printf("[INFO] pgpipeline backup data done\n")
	if backupCtx.fail > 0 {
		return errors.New("backup data failed")
	}
	return nil
}

func backupFile(ctx *BackupCtx, filePath string) error {
	ctx.queue.AddTask(NewTask(filePath))
	err := countBackupSize(ctx, filePath)
	if err != nil {
		return err
	}
	err = backupWorkflow(ctx)
	return err
}

func backupControl(ctx *BackupCtx, param map[string]interface{}) error {
	ctx.logger.Printf("[INFO] pgpipeline backup pg_control\n")
	err := backupFile(ctx, "global/pg_control")
	return err
}

func backupBackupLabel(backupCtx *BackupCtx, params map[string]interface{}) error {
	// backup backup label in block backup mode
	if !backupCtx.useBlock {
		return nil
	}

	backupCtx.logger.Printf("[INFO] pgpipeline backup backup_label\n")
	backupLabel, err := getBackupLabelName(backupCtx)
	if err != nil {
		return err
	}

	err = backupFile(backupCtx, backupLabel)
	return err
}

func isDigit(str string) bool {
	for i := 0; i < len(str); i++ {
		if str[i] < '0' || str[i] > '9' {
			return false
		}
	}
	return true
}

func checkRelFile(path string) bool {
	sps := strings.Split(path, "/")
	// TODO more tablespace ?
	if sps[0] == "base" || sps[0] == "global" {
		if isDigit(sps[len(sps)-1]) {
			return true
		}
	}
	return false
}

func filterFiles(backupCtx *BackupCtx, files []string) []string {
	var ffiles []string
	for _, file := range files {
		if len(file) >= 9 && file[0:9] == ".s.PGSQL." {
			backupCtx.logger.Printf("[INFO] Found socket file: %s, ignore it\n", file)
			continue
		}
		ffiles = append(ffiles, file)
	}
	return ffiles
}

/*
 * ensure that increbackup plugin has backuped all the wals before current wal.
 */
func syncWals(backupCtx *BackupCtx) error {
	// switch wal
	curwal, _, err := getCurrentWalFile(backupCtx)
	if err != nil {
		return err
	}

	backupCtx.logger.Printf("[INFO] execute pgSwitchWal\n")
	err = pgSwitchWal(backupCtx.bkdbinfo)
	if err != nil {
		backupCtx.logger.Printf("[ERROR] pgSwitchWal, failed: %s\n", err.Error())
		return err
	}
	backupCtx.logger.Printf("[INFO] execute pgSwitchWal success\n")

	time.Sleep(2 * time.Second)

	// wait wals that before switched wal to be backuped
	dir := "/pg_wal/archive_status/"
	files, err := ReadDir(backupCtx, backupCtx.frontend, dir)
	if err != nil {
		backupCtx.logger.Printf("[ERROR] read dir failed. path[%s], error[%s]\n", dir, err.Error())
		return err
	}

	type wal struct {
		name string
		done bool
	}

	var oldwals []wal
	for _, file := range files {
		fileSuffix := path.Ext(file)
		if fileSuffix == ".ready" {
			tmp := strings.TrimSuffix(file, ".ready")
			sps := strings.Split(tmp, "/")
			walname := sps[len(sps)-1]
			if walname <= curwal {
				walfile := wal{name: walname, done: false}
				oldwals = append(oldwals, walfile)
				backupCtx.logger.Printf("[INFO] append wal: %s since less than %s\n", walfile, curwal)
			}
		}
	}

	len := len(oldwals)
	count := 0
	tryTimes := len * 100
	for {
		time.Sleep(1 * time.Second)

		for i := 0; i < len; i++ {
			if !oldwals[i].done {
				isFound, err := pgCountWal(backupCtx.bkdbinfo, backupCtx.status.InstanceID, oldwals[i].name)
				if err != nil {
					return err
				}

				if isFound {
					oldwals[i].done = true
					count = count + 1
					backupCtx.logger.Printf("[INFO] backup wal %s done, require rest: %d wal to be backuped\n", oldwals[i].name, len-count)
				}
			}
		}

		if count == len {
			break
		}

		tryTimes = tryTimes - 1
		if tryTimes == 0 {
			break
		}
	}

	if count != len {
		return errors.New("wait too long for backuping wals, exit")
	}

	return nil
}

/*
 * remove redundant file in block backup directory before full backup
 */
func resetBlockBackup(backupCtx *BackupCtx) error {
	/**
	 * Sync wals to reduce the redundant block index files for block backup.
	 * Don't let it block full backup since it is just a optimization.
	 */
	err := syncWals(backupCtx)
	if err != nil {
		backupCtx.logger.Printf("[WARNING] sync wals failed: %s\n", err.Error())
	}

	NotifyManagerBlockHelperCallback(backupCtx, "clearblockdir")
	time.Sleep(1 * time.Second) // TODO sleeping to wait task done is trival

	return err
}

/*
 * stop sync the block change to local until block backup finished
 */
func stopBlockSync(backupCtx *BackupCtx) error {
	err := syncWals(backupCtx)
	if err != nil {
		return err
	}

	err = setBlockBackupFlag(backupCtx.bkdbinfo)
	if err != nil {
		backupCtx.logger.Printf("[ERROR] set pg block backup status failed: %s\n", err.Error())
	}

	return err
}

/*
 * clear files in block backup directory and free the lock for continuing sync block change to local
 */
func resetBlockSync(backupCtx *BackupCtx) error {
	err := NotifyManagerBlockHelperCallback(backupCtx, "clearblockindex")
	if err != nil {
		return err
	}
	time.Sleep(1 * time.Second)

	return setBlockBackupDone(backupCtx.bkdbinfo)
}

func backupAction(backupCtx *BackupCtx) error {
	backupCtx.logger.Printf("[INFO] pgpipeline wait full backup done\n")
	waitFullBackupDone(backupCtx.bkdbinfo)

	backupCtx.logger.Printf("[INFO] pgpipeline set full backup flag\n")
	err := setFullBackupFlag(backupCtx.bkdbinfo)
	if err != nil {
		backupCtx.logger.Printf("[ERROR] set pg fullbackup status failed: %s\n", err.Error())
	}
	defer setFullBackupDone(backupCtx.bkdbinfo)

	if backupCtx.enableBlockBackup && !backupCtx.useBlock {
		backupCtx.logger.Printf("[INFO] reset block backup ... \n")
		err = resetBlockBackup(backupCtx)
		if err != nil {
			backupCtx.logger.Printf("[ERROR] reset block backup failed: %s\n", err.Error())
			return err
		}
		backupCtx.logger.Printf("[INFO] reset block backup done \n")
	}

	backupCtx.logger.Printf("[INFO] try execute pgStartBackup\n")
	err = pgStartBackup(backupCtx.bkdbinfo)
	if err != nil {
		backupCtx.logger.Printf("[ERROR] pgStartBackup, failed: %s\n", err.Error())
		return err
	}
	isStopbackup := false
	defer func() {
		if !isStopbackup {
			err := pgStopBackup(backupCtx.bkdbinfo)
			if err != nil {
				backupCtx.logger.Printf("[ERROR] pgStopBackup, failed: %s\n", err.Error())
			}
		}
	}()
	backupCtx.logger.Printf("[INFO] execute pgStartBackup success\n")

	if backupCtx.useBlock {
		backupCtx.logger.Printf("[INFO] stop block sync ... \n")
		err = stopBlockSync(backupCtx)
		if err != nil {
			backupCtx.logger.Printf("[ERROR] stop block sync failed: %s\n", err.Error())
			return err
		}
		backupCtx.logger.Printf("[INFO] stop block sync done \n")
		defer setBlockBackupDone(backupCtx.bkdbinfo)

		increbkip, err := getIncrementalBackupIp(backupCtx)
		if err != nil {
			backupCtx.logger.Printf("[ERROR] get incremental backup ip failed: %s\n", err.Error())
			return err
		}
		backupCtx.logger.Printf("[INFO] manager get incremental backup ip: %s\n", increbkip)
		backupCtx.incrementalip = increbkip
		backupCtx.blockCtx = &HTTPCtx{
			endpoint: increbkip + backupCtx.conf.BlockServerPort,
			instance: backupCtx.status.InstanceID,
			path:     backupCtx.status.InstanceID,
		}
	}

	params := make(map[string]interface{})

	backupCtx.status.Stage = "Running"

	if backupCtx.conf.LocalFileDir != "" {
		err = backupLocalFiles(backupCtx, params, backupCtx.conf.LocalFileDir, "localfiles")
		if err != nil {
			backupCtx.status.Stage = "Failed"
			backupCtx.logger.Printf("[ERROR] pgpipeline backup local files failed, failed: %s\n", err.Error())
			return err
		}
	}

	if backupCtx.job.DBClusterMetaDir != "" {
		err = backupLocalFiles(backupCtx, params, backupCtx.job.DBClusterMetaDir, "DBClusterMeta")
		if err != nil {
			backupCtx.status.Stage = "Failed"
			backupCtx.logger.Printf("[ERROR] pgpipeline backup DBClusterMetaDir files failed, failed: %s\n", err.Error())
			return err
		}
	}

	params["path"] = "/"
	_files, err := backupCtx.frontend.funcs["readdir"](backupCtx.frontend.handle, params)
	if err != nil {
		backupCtx.status.Stage = "Failed"
		return err
	}
	files, ok := _files.([]string)
	if !ok {
		return errors.New("readdir must be return []string")
	}

	err = backupBackupLabel(backupCtx, params)
	if err != nil {
		return err
	}

	// TODO estimate block backup
	err = estimateBackupSize(backupCtx)
	if err != nil {
		return err
	}

	files = filterFiles(backupCtx, files)

	err = backupData(backupCtx, files, params)
	if err != nil {
		return err
	}

	err = backupControl(backupCtx, params)
	if err != nil {
		return err
	}

	err = backupDma(backupCtx, params)
	if err != nil {
		return err
	}

	// get startwal info before pgstopbackup since pgstopbackup will remove backup_label
	startWalName, err := getStartWalName(backupCtx)
	if err != nil {
		return err
	}

	isStopbackup = true
	err = pgStopBackup(backupCtx.bkdbinfo)
	if err != nil {
		backupCtx.logger.Printf("[ERROR] pgStopBackup, failed: %s\n", err.Error())
		return err
	}

	err = genMinRecoveryTime(backupCtx)
	if err != nil {
		return err
	}

	// TODO no need to backup wal before start wal
	err = backupLog(backupCtx, params, startWalName)
	if err != nil {
		return err
	}

	backupCtx.status.EndTime = time.Now().Unix()

	if backupCtx.id != "" {
		meta, err := genMetaInfo(backupCtx, startWalName)
		if err != nil {
			return err
		}
		backupCtx.logger.Printf("[INFO] gen MetaInfo\n")

		err = backupMeta(backupCtx, params, meta)
		if err != nil {
			return err
		}
		backupCtx.logger.Printf("[INFO] backup Meta\n")

		err = backupMetaToLocal(backupCtx, meta)
		if err != nil {
			return err
		}
		backupCtx.logger.Printf("[INFO] backup Meta to local\n")
	}

	if backupCtx.stop {
		backupCtx.logger.Printf("[ERROR] recieve stop command when backup\n")
		err = errors.New("recieve stop command when backup")
		return err
	}

	err = updateFullMeta(backupCtx)
	if err != nil {
		backupCtx.logger.Printf("[INFO] update full meta error %s\n", err)
		return err
	}
	backupCtx.logger.Printf("[INFO] update full backup meta\n")

	if backupCtx.cmdline {
		backupCtx.status.Stage = "Finished"
		err := recordFullBackupToMultiRemoteDB(backupCtx)
		if err != nil {
			return err
		}
	}

	if backupCtx.useBlock {
		backupCtx.logger.Printf("[INFO] reset block sync ... \n")
		err = resetBlockSync(backupCtx)
		if err != nil {
			backupCtx.logger.Printf("[ERROR] reset block sync failed: %s\n", err.Error())
			return err
		}
		backupCtx.logger.Printf("[INFO] reset block sync done \n")
	}

	return nil
}

// func OpenFile(ctx *BackupCtx, fs fileSystem, filename string) (interface{}, error) {
//     param := make(map[string]interface{})
//     param["name"] = filename
//     param["flags"] = uint(00)
//     param["mode"] = uint(0400)
//     fi, err := fs.funcs["open"](fs.handle, param)

//     if err != nil {
//         ctx.logger.Printf("[ERROR] fs[%s] open[%s] failed, err[%s]\n", fs.name, filename, err.Error())
//         return nil, err
//     }

//     return fi, nil
// }

// func CreateFile(ctx *BackupCtx, fs fileSystem, filename string) (interface{}, error) {
//     param := make(map[string]interface{})
//     param["name"] = filename
//     fi, err := fs.funcs["create"](fs.handle, param)
//     if err != nil {
//         ctx.logger.Printf("[ERROR] fs[%s] create[%s] failed, err[%s]\n", fs.name, filename, err.Error())
//         return nil, err
//     }

//     return fi, nil
// }

// func Close(ctx *BackupCtx, fs fileSystem, fp interface{}) {
//     param := make(map[string]interface{})
//     param["file"] = fp
//     fs.funcs["close"](fs.handle, param)
// }

func genMinRecoveryTime(ctx *BackupCtx) error {
	ctx.minRecoveryTime = time.Now().Unix() + 1
	return nil
}

func updateFullMeta(ctx *BackupCtx) error {
	params := make(map[string]interface{})
	params["path"] = "fullbackups.pg_o_backup_meta"
	ret, err := ctx.metaFile.funcs["exist"](ctx.metaFile.handle, params)
	if err != nil {
		ctx.logger.Printf("[ERROR] pgpipeline exist %s failed: %s\n", params["path"], err.Error())
		return err
	}

	fulltype := "full"
	if ctx.useBlock {
		fulltype = "block"
	}

	if ret.(bool) {
		metaFile, err := OpenFile(ctx.metaFile, "fullbackups.pg_o_backup_meta")
		if err != nil {
			ctx.logger.Printf("[ERROR] open meta file failed. err[%s]\n", err.Error())
			return err
		}

		metaReader, ok := metaFile.(io.Reader)
		if !ok {
			err = errors.New("invalid metaFile")
			return err
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
		Close(ctx.metaFile, metaFile)

		params["name"] = "fullbackups.pg_o_backup_meta"
		_, err = ctx.metaFile.funcs["unlink"](ctx.metaFile.handle, params)
		if err != nil {
			ctx.logger.Printf("[ERROR] fs[%s] unlink[%s] failed, err[%s]\n", ctx.metaFile.name, params["name"], err.Error())
			return err
		}

		backendFile, err := CreateFile(ctx.metaFile, "fullbackups.pg_o_backup_meta")
		if err != nil {
			ctx.logger.Printf("[ERROR] create metaFile file fullbackups.pg_o_backup_meta failed, err[%s]\n", err.Error())
			return err
		}
		defer Close(ctx.metaFile, backendFile)

		backendWriter, ok := backendFile.(io.Writer)
		if !ok {
			err = errors.New("invalid backendFile")
			return err
		}

		i := 0
		for ; i < len(wbuf); i++ {
			if wbuf[i] == '\n' {
				break
			}
		}
		n, err := strconv.Atoi(string(wbuf[0:i]))
		if err != nil {
			ctx.logger.Printf("[ERROR] convert first line of fullbackups.pg_o_backup_meta failed, err[%s]\n", err.Error())
			return err
		}

		newn := 1
		if newn >= ctx.conf.MaxFullsRecord {
			newn = ctx.conf.MaxFullsRecord
			backendWriter.Write([]byte(strconv.Itoa(newn) + "\n"))
			record := ctx.status.BackupID + "," + strconv.FormatUint(uint64(ctx.status.StartTime), 10) + "," + strconv.FormatUint(uint64(ctx.status.EndTime), 10) + "," + fulltype + "\n"
			buf := []byte(record)
			_, ew := backendWriter.Write(buf)
			if ew != nil {
				return ew
			}
		} else {
			res := ctx.conf.MaxFullsRecord - newn
			var bindex, eindex, boffset, eoffset int
			eindex = n
			if res >= n {
				bindex = 1
			} else {
				bindex = n - res + 1
			}

			index := 0
			for ; i < len(wbuf); i++ {
				if wbuf[i] == '\n' {
					index++
					if index == bindex {
						boffset = i + 1
					}
					if index == eindex+1 {
						eoffset = i
						break
					}
				}
			}
			if boffset >= eoffset {
				return errors.New("[ERROR] parse fulls meta failed, boffset is larger than eoffset")
			}

			backendWriter.Write([]byte(strconv.Itoa(newn+eindex-bindex+1) + "\n"))
			_, err := backendWriter.Write(wbuf[boffset : eoffset+1])
			if err != nil {
				return err
			}

			record := ctx.status.BackupID + "," + strconv.FormatUint(uint64(ctx.status.StartTime), 10) + "," + strconv.FormatUint(uint64(ctx.status.EndTime), 10) + "," + fulltype + "\n"
			buf := []byte(record)
			_, ew := backendWriter.Write(buf)
			if ew != nil {
				return ew
			}
		}
	} else {
		n := 1

		backendFile, err := CreateFile(ctx.metaFile, "fullbackups.pg_o_backup_meta")
		if err != nil {
			ctx.logger.Printf("[ERROR] create backend file fullbackups.pg_o_backup_meta failed, err[%s]\n", err.Error())
			return err
		}
		defer Close(ctx.metaFile, backendFile)

		backendWriter, ok := backendFile.(io.Writer)
		if !ok {
			err = errors.New("invalid backendFile")
			return err
		}

		if n > ctx.conf.MaxFullsRecord {
			n = ctx.conf.MaxFullsRecord
		}

		backendWriter.Write([]byte(strconv.Itoa(n) + "\n"))
		record := ctx.status.BackupID + "," + strconv.FormatUint(uint64(ctx.status.StartTime), 10) + "," + strconv.FormatUint(uint64(ctx.status.EndTime), 10) + "," + fulltype + "\n"
		buf := []byte(record)
		_, ew := backendWriter.Write(buf)
		if ew != nil {
			return ew
		}
	}
	return nil
}

func updateCRC(ctx *BackupCtx) error {
	for i := 0; i < len(ctx.pgFiles); i++ {
		if crc, ok := ctx.crcMap.Load(ctx.pgFiles[i].Name); ok {
			ctx.pgFiles[i].Crc = crc.(uint32)
		}
	}
	return nil
}

func genMetaInfo(ctx *BackupCtx, startWalName string) ([]byte, error) {
	var files []string
	for _, file := range ctx.tarFiles {
		files = append(files, "\""+file+"\"")
	}

	filesstr := strings.Join(files, ",")
	filesstr = "[" + filesstr + "]"

	backupSize := ctx.status.BackupSize

	_, _, err := getLastCheckPointTime(ctx)
	if err != nil {
		return nil, err
	}

	backupEndTime := getBackupEndTime()

	err = updateCRC(ctx)
	if err != nil {
		return nil, err
	}

	metaInfo := make(map[string]interface{})
	metaInfo["Files"] = ctx.tarFiles
	metaInfo["BackupSize"] = strconv.FormatInt(backupSize, 10)
	metaInfo["StartWalName"] = startWalName
	metaInfo["MinRecoveryTime"] = convertUnixToTime(ctx.minRecoveryTime)
	metaInfo["MinRecoveryTimeUnix"] = ctx.minRecoveryTime
	/*
	 * Instead LastCheckPointTime of MinRecoveryTime temporarily for polarstack
	 */
	metaInfo["LastCheckPointTime"] = convertUnixToTime(ctx.minRecoveryTime)
	metaInfo["LastCheckPointTimeUnix"] = ctx.minRecoveryTime
	metaInfo["BackupEndTime"] = backupEndTime
	metaInfo["LocalFile"] = ctx.localTarFile
	metaInfo["BackupStartTimeUnix"] = ctx.status.StartTime
	metaInfo["BackupEndTimeUnix"] = ctx.status.EndTime
	metaInfo["UseBlock"] = ctx.useBlock
	metaInfo["PGFiles"] = ctx.pgFiles

	content, err := json.MarshalIndent(&metaInfo, "", "\t")
	if err != nil {
		return nil, err
	}

	return content, nil
}

func getLastCheckPointTime(ctx *BackupCtx) (string, int64, error) {
	control := "global/pg_control"
	content, err := getFileContent(ctx, control)
	if err != nil {
		ctx.logger.Printf("[ERROR] get pg_control content failed: %s\n", err.Error())
		return "", 0, err
	}
	pgctrl, err := ParsePgControl(content)
	if err != nil {
		ctx.logger.Printf("[ERROR] parse pg_control content failed: %s\n", err.Error())
		return "", 0, err
	}

	var now time.Time
	now = time.Unix(int64(pgctrl.starttime), 0)
	t := now.Format("2006-01-02 15:04:05")
	zone, _ := now.Zone()
	return t + " " + zone, now.Unix(), nil
}

func getBackupEndTime() string {
	now := time.Now()
	t := now.Format("2006-01-02 15:04:05")
	zone, _ := now.Zone()
	return t + " " + zone
}

func getBackupLabelName(ctx *BackupCtx) (string, error) {
	var backupLabel string

	params := make(map[string]interface{})
	params["path"] = "polar_exclusive_backup_label"
	params["mode"] = F_OK
	ret, err := ctx.frontend.funcs["access"](ctx.frontend.handle, params)
	if err != nil {
		ctx.logger.Printf("[ERROR] pgpipeline access %s failed: %s\n", params["path"], err.Error())
		return "", err
	}
	if ret == 0 {
		backupLabel = "polar_exclusive_backup_label"
	} else {
		params["path"] = "backup_label"
		params["mode"] = F_OK
		ret, err := ctx.frontend.funcs["access"](ctx.frontend.handle, params)
		if err != nil {
			ctx.logger.Printf("[ERROR] pgpipeline access %s failed: %s\n", params["path"], err.Error())
			return "", err
		}
		if ret == 0 {
			backupLabel = "backup_label"
		} else {
			ctx.logger.Printf("[ERROR] no polar_exclusive_backup_label or backup_label exist")
			return "", errors.New("no polar_exclusive_backup_label or backup_label exist")
		}
	}

	return backupLabel, nil
}

func getStartWalName(ctx *BackupCtx) (string, error) {
	var backupLabel string

	backupLabel, err := getBackupLabelName(ctx)
	if err != nil {
		ctx.logger.Printf("[ERROR] get backup label name failed: %s\n", err.Error())
		return "", err
	}

	content, err := getFileContent(ctx, backupLabel)
	if err != nil {
		ctx.logger.Printf("[ERROR] get %s content failed: %s\n", backupLabel, err.Error())
		return "", err
	}

	contentStr := string(content)
	idx := strings.Index(contentStr, "\n")
	if idx == -1 {
		return "", errors.New("backuplabel is null")
	}
	fistLine := contentStr[:idx]

	size := len(fistLine)
	foundpos := -1
	for pos := 0; pos < size-10; pos++ {
		if fistLine[pos:pos+4] == "file" {
			foundpos = pos
			break
		}
	}
	var walname string
	if foundpos == -1 || foundpos+29 >= size {
		return "", errors.New("backuplabel not contain wal file name")
	} else {
		walname = fistLine[foundpos+5 : foundpos+29]
	}

	return walname, nil
}

func checkRestoreFiles(files []string, compress bool) bool {
	prefixs := make(map[string]string)
	for _, file := range files {
		ext := filepath.Ext(file)
		if ext == "."+META_EXT {
			continue
		}
		if compress {
			if ext != ".lz4" {
				return false
			}
		} else {
			if ext != ".tar" {
				return false
			}
		}
		prefix := strings.Split(file, ".")[0]
		prefixs[prefix] = prefix
	}
	count := 0
	for _, _ = range prefixs {
		count++
	}
	if count > 1 {
		return false
	}
	return true
}

func getRestoreFiles(ctx *BackupCtx) ([]string, int64, error) {
	filename := getMetaName(ctx)
	params := make(map[string]interface{})
	params["name"] = filename
	_file, err := ctx.backend.funcs["open"](ctx.backend.handle, params)
	if err != nil {
		ctx.logger.Printf("[ERROR] pgpipeline open backup meta %s failed: %s\n", filename, err.Error())
		return nil, 0, err
	}

	params["file"] = _file
	defer ctx.backend.funcs["close"](ctx.backend.handle, params)

	reader := _file.(io.Reader)
	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		err = errorEnhance(err)
		ctx.logger.Printf("[ERROR] pgpipeline read backup meta %s failed: %s\n", filename, err.Error())
		return nil, 0, err
	}

	var backupMeta BackupMeta
	err = json.Unmarshal(buf, &backupMeta)
	if err != nil {
		return nil, 0, errors.New("parse backupMeta failed")
	}

	ctx.pgFiles = backupMeta.PGFiles
	for _, file := range ctx.pgFiles {
		ctx.crcMap.Store(file.Name, file.Crc)
	}

	ctx.useBlock = backupMeta.UseBlock

	backupSize, err := strconv.ParseInt(backupMeta.BackupSize, 10, 64)
	if err != nil {
		return nil, 0, errors.New("convert backupsize in backupMeta failed")
	}

	return backupMeta.Files, backupSize, nil
}

func getLocalFile(ctx *BackupCtx) (string, error) {
	filename := getMetaName(ctx)
	params := make(map[string]interface{})
	params["name"] = filename
	_file, err := ctx.backend.funcs["open"](ctx.backend.handle, params)
	if err != nil {
		ctx.logger.Printf("[ERROR] pgpipeline open backup meta %s failed: %s\n", filename, err.Error())
		return "", err
	}

	params["file"] = _file
	defer ctx.backend.funcs["close"](ctx.backend.handle, params)

	reader := _file.(io.Reader)
	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		err = errorEnhance(err)
		ctx.logger.Printf("[ERROR] pgpipeline read backup meta %s failed: %s\n", filename, err.Error())
		return "", err
	}

	var backupMeta BackupMeta
	err = json.Unmarshal(buf, &backupMeta)
	if err != nil {
		return "", errors.New("parse backupMeta failed")
	}

	return backupMeta.LocalFile, nil
}

func getRestoreSize(ctx *BackupCtx, files []string) (int64, error) {
	for _, file := range files {
		ext := filepath.Ext(file)
		if ext == "."+META_EXT {
			params := make(map[string]interface{})
			params["name"] = file
			_file, err := ctx.backend.funcs["open"](ctx.backend.handle, params)
			if err != nil {
				ctx.logger.Printf("[ERROR] pgpipeline open backup meta %s failed: %s\n", file, err.Error())
				return 0, err
			}

			params["file"] = _file

			reader := _file.(io.Reader)
			buf, err := ioutil.ReadAll(reader)
			if err != nil {
				ctx.logger.Printf("[ERROR] pgpipeline read backup meta %s failed: %s\n", file, err.Error())
				ctx.backend.funcs["close"](ctx.backend.handle, params)
				return 0, err
			}

			var backupMeta BackupMeta
			err = json.Unmarshal(buf, &backupMeta)
			if err != nil {
				ctx.backend.funcs["close"](ctx.backend.handle, params)
				return 0, errors.New("parse backupMeta failed:" + err.Error())
			}

			backupSize, err := strconv.ParseInt(backupMeta.BackupSize, 10, 64)
			if err != nil {
				ctx.backend.funcs["close"](ctx.backend.handle, params)
				return 0, errors.New("convert backupsize in backupMeta failed")
			}

			ctx.backend.funcs["close"](ctx.backend.handle, params)
			return backupSize, nil
		}
	}
	return 0, errors.New("not found meta file")
}

func restoreAction(backupCtx *BackupCtx) error {
	params := make(map[string]interface{})
	var files []string
	var backupSize int64
	var err error

	if backupCtx.id != "" {
		files, backupSize, err = getRestoreFiles(backupCtx)
		if err != nil {
			return err
		}
	} else {
		params["path"] = ""
		_files, err := backupCtx.backend.funcs["readdir"](backupCtx.backend.handle, params)
		if err != nil {
			return err
		}
		files = _files.([]string)
		valid := checkRestoreFiles(files, backupCtx.compress)
		if !valid {
			backupCtx.logger.Printf("[ERROR] pgpipeline restore files is not a full backup sets")
			return errors.New("restore files invalid")
		}
		backupSize, err = getRestoreSize(backupCtx, files)
		if err != nil {
			return err
		}
	}

	for _, file := range files {
		ext := filepath.Ext(file)
		if ext == "."+META_EXT {
			continue
		}
		backupCtx.queue.AddTask(NewTask(file))
	}

	backupCtx.status.AllFilesCount = len(files)
	backupCtx.status.Stage = "Running"
	backupCtx.status.BackupSize = backupSize
	var wait sync.WaitGroup
	uploadDone := make(chan struct{})
	parallelRestoreWorkflow(backupCtx, backupCtx.workerCount, &wait)
	go func() {
		wait.Wait()
		close(uploadDone)
	}()
	waitUntilStop(backupCtx, uploadDone)

	fail := atomic.LoadInt32(&backupCtx.fail)
	if fail > 0 {
		return errors.New("restore failed")
	}

	if backupCtx.useBlock {
		backupCtx.logger.Printf("[INFO] remove redundant files ...\n")
		err = removeRedundantFiles(backupCtx)
		if err != nil {
			backupCtx.status.Stage = "Failed"
			backupCtx.logger.Printf("[ERROR] pgpipeline remove redundant files failed, failed: %s\n", err.Error())
			return err
		}
		backupCtx.logger.Printf("[INFO] remove redundant files done\n")
	}

	if backupCtx.job.DBClusterMetaDir != "" {
		err = restoreLocalFiles(backupCtx, params, backupCtx.job.DBClusterMetaDir, "DBClusterMeta")
		if err != nil {
			backupCtx.status.Stage = "Failed"
			backupCtx.logger.Printf("[ERROR] pgpipeline restore DBClusterMetaDir files failed, failed: %s\n", err.Error())
			return err
		}
	}

	if backupCtx.stop {
		backupCtx.status.Action = "restore"
		backupCtx.logger.Printf("[ERROR] recieve stop command when restore\n")
		err = errors.New("recieve stop command when restore")
		return err
	}

	backupCtx.status.Stage = "Finished"
	return nil
}

func removeRedundantFiles(backupCtx *BackupCtx) error {
	set := make(map[string]struct{})
	for _, file := range backupCtx.pgFiles {
		set[file.Name] = struct{}{}
	}

	param := make(map[string]interface{})
	files, err := listdirRecursion("/", param, &backupCtx.frontend)
	if err != nil {
		backupCtx.logger.Printf("[ERROR] pgpipeline listdirRecursion %s failed: %s\n", "/", err.Error())
		return err
	}

	backupCtx.logger.Printf("[INFO] size of local files:%d, size of record files:%d\n", len(files), len(backupCtx.pgFiles))

	var unusedDirs []string
	for _, file := range files {
		if _, ok := set[file]; !ok {
			param["path"] = file
			// TODO unlink dir ?
			isdir, err := backupCtx.frontend.funcs["isdir"](backupCtx.frontend.handle, param)
			if err != nil {
				return err
			}
			if !isdir.(bool) {
				param["name"] = file
				_, err := backupCtx.frontend.funcs["unlink"](backupCtx.frontend.handle, param)
				if err != nil {
					backupCtx.logger.Printf("[ERROR] pgpipgline frontend unlink %s failed: %s\n", file, err.Error())
					return err
				}
				backupCtx.logger.Printf("[INFO] pgpipgline frontend unlink %s done\n", file)
			} else {
				if file != "" {
					unusedDirs = append(unusedDirs, file)
				}
			}
		}
	}

	for _, dir := range unusedDirs {
		param["path"] = dir
		_, err := backupCtx.frontend.funcs["rmdir"](backupCtx.frontend.handle, param)
		if err != nil {
			backupCtx.logger.Printf("[ERROR] pgpipgline frontend rmdir %s failed: %s\n", dir, err.Error())
			return err
		}
		backupCtx.logger.Printf("[INFO] pgpipgline frontend rmdir %s done\n", dir)
	}

	return nil
}

func restoreLocalFiles(backupCtx *BackupCtx, params map[string]interface{}, folder string, name string) error {
	frontend := backupCtx.frontend
	backupCtx.frontend = backupCtx.localFile
	defer func() {
		backupCtx.frontend = frontend
	}()

	localfile, err := getLocalFile(backupCtx)
	if err != nil {
		backupCtx.logger.Printf("[ERROR] pgpipeline get local files failed: %s\n", err.Error())
		return err
	}
	backupCtx.queue.AddTask(NewTask(localfile))

	err = _restoreWorkflow(backupCtx, NOTCHECKCRC)
	if err != nil {
		backupCtx.logger.Printf("[ERROR] pgpipeline restore local files %s failed: %s\n", localfile, err.Error())
		return err
	}

	backupCtx.logger.Printf("[INFO] pgpipeline restore local files %s done\n", localfile)
	return nil
}

func getTarName(ctx *BackupCtx) string {
	index := atomic.AddInt32(&ctx.tarIndex, 1)
	if ctx.compress {
		return ctx.id + "." + strconv.Itoa(int(index)) + ".tar.lz4"
	} else {
		return ctx.id + "." + strconv.Itoa(int(index)) + ".tar"
	}
}

func recordFullBackupToMultiRemoteDB(ctx *BackupCtx) error {
	err := recordFullBackupToRemoteDB(ctx, ctx.bkdbinfo)
	if err != nil {
		return err
	}

	if ctx.mtdbinfo != nil {
		err := recordFullBackupToRemoteDB(ctx, ctx.mtdbinfo)
		if err != nil {
			return err
		}
	}

	return nil
}

func recordFullBackupToRemoteDB(ctx *BackupCtx, dbinfo *DBInfo) error {
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
	rows, err := queryDB(dbinfo, sqlStatement)
	defer rows.Close()
	if err != nil {
		return err
	}

	rs := &ctx.status

	var sql string
	if dbinfo.dbType == "pgsql" {
		sql = `
        INSERT INTO backup_meta (instanceid, backupid, backupjobid, file, backuptype, starttime, endtime, location, status)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	} else if dbinfo.dbType == "mysql" {
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

	_, err = dbinfo.db.Exec(sql, rs.InstanceID, rs.BackupID, rs.BackupJobID, rs.ID, backuptype, rs.StartTime, rs.EndTime, rs.Location, rs.Stage)
	if err != nil {
		return err
	}

	return nil
}

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/pierrec/lz4"
	"io"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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
		return nil, errors.New("ctx.imports must be *sync.Map error)")
	}

	backupCtx.funcs = funcs
	backupCtx.frontend = fileSystem{}
	backupCtx.backend = fileSystem{}

	backupCtx.stopEvent = stop
	backupCtx.queue = NewDispatchTask(initConf.HostIP, initConf.BackupNodes)
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
	backupCtx.logger = logger

	if len(backupCtx.conf.ManagerAddr) > 0 {
		backupCtx.manager = backupCtx.conf.ManagerAddr[0]
		if len(strings.Split(backupCtx.manager, ":")) == 1 {
			backupCtx.manager = backupCtx.manager + ":" + DefaultServerPort
		}
	}

	return backupCtx, nil
}

// param:
func PluginRun(ctx interface{}, param interface{}) error {
	backupCtx, ok := ctx.(*BackupCtx)
	if !ok {
		return errors.New("ctx must be *BackupCtx")
	}

	if backupCtx == nil {
		return errors.New("ctx must not be nil")
	}

	CapturePanic(backupCtx)

	action, extraInfo, err := getActionFromTask(backupCtx, param)
	if err != nil {
		return err
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
		return errors.New("param must be map[string]interface{}")
	}

	//init frontend handle
	var extern map[string]interface{}
	_extern, ok := m["extern"]
	if ok {
		extern, ok = _extern.(map[string]interface{})
		if !ok {
			return errors.New("param.extern must be map[string]interface{}")
		}
	}

	err = initTask(backupCtx, extern)
	if err != nil {
		backupCtx.logger.Printf("[ERROR] pipeline init task failed, exit now\n")
		backupCtx.status.Error = err.Error()
		backupCtx.status.Stage = "Failed"
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
	}

	if backupCtx.action == "stop" {
		backupCtx.stop = true
		backupCtx.status.Stage = "Stopped"
		NotifyManagerCallback(backupCtx, "Stopped")
		return nil
	}

	backupCtx.status.Stage = "Preparing"
	backupCtx.status.Node = backupCtx.hostip
	backupCtx.status.StartTime = time.Now().Unix()
	backupCtx.status.EndTime = backupCtx.status.StartTime
	backupCtx.status.Backend = backupCtx.job.Backend
	backupCtx.status.Type = "Full"

	backupCtx.status.BackupJobID = backupCtx.job.BackupJobID
	backupCtx.status.BackupID = backupCtx.job.BackupID
	backupCtx.status.InstanceID = backupCtx.job.InstanceID
	backupCtx.status.Callback = backupCtx.job.CallbackURL

	err = initFunction(backupCtx, backupCtx.job.Frontend, "Frontend")
	if err != nil {
		backupCtx.status.Error = err.Error()
		backupCtx.status.Stage = "Failed"
		NotifyManagerCallback(backupCtx, "Failed")
		return err
	}
	err = initFunction(backupCtx, backupCtx.job.Backend, "Backend")
	if err != nil {
		backupCtx.status.Error = err.Error()
		backupCtx.status.Stage = "Failed"
		NotifyManagerCallback(backupCtx, "Failed")
		return err
	}

	backupCtx.logger.Printf("[INFO] pipeline start [report] goroutine, report addr: %s\n", backupCtx.manager)
	go func() {
		stop := false
		for !stop {
			if backupCtx.stop {
				backupCtx.logger.Printf("[INFO] pipeline %s stop, [report] goroutine exit\n", backupCtx.action)
				stop = true
			}
			backupCtx.logger.Printf("[INFO] pipeline [report] %s cmd status\n", backupCtx.action)
			NotifyManagerCallback(backupCtx, backupCtx.status.Stage)
			time.Sleep(5 * time.Second)
		}
	}()

	backupCtx.logger.Printf("[INFO] pipeline init dependence conf begin...\n")
	err = initDependence(backupCtx, extern)
	if err != nil {
		finiDependence(backupCtx)
		backupCtx.logger.Printf("[ERROR] pipeline init dependence failed: %s\n", err.Error())
		return err
	}
	backupCtx.logger.Printf("[INFO] pipeline init dependence success\n")

	action = backupCtx.action
	backupCtx.status.Stage = "Running"
	if action == "backup" {
		backupCtx.logger.Printf("[INFO] pipeline do backup now\n")
		// compress info need to send to backend for restore
		err = backupAction(backupCtx)
	} else if action == "restore" {
		backupCtx.logger.Printf("[INFO] pipeline do restore now\n")
		err = restoreAction(backupCtx)
	} else {
		backupCtx.logger.Printf("[ERROR] pipeline invalid action: %s\n", backupCtx.action)
	}

	if err != nil {
		backupCtx.status.Error = err.Error()
		backupCtx.status.Stage = "Failed"
		backupCtx.logger.Printf("[ERROR] pipeline %s failed: %s\n", backupCtx.action, err.Error())
	} else {
		backupCtx.status.Stage = "Finished"
	}

	finiDependence(backupCtx)
	backupCtx.logger.Printf("[INFO] pipelinefini dependence plugin\n")
	return nil
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
		}
	}
	return nil
}

func parallelBackupWorkflow(backupCtx *BackupCtx, worker int, wait *sync.WaitGroup) {
	for i := 0; i < worker; i++ {
		wait.Add(1)
		go func() {
			err := backupWorkflow(backupCtx, wait)
			if err != nil {
				atomic.AddInt32(&backupCtx.fail, 1)
			}
		}()
	}
}

func parallelRestoreWorkflow(backupCtx *BackupCtx, worker int, wait *sync.WaitGroup) {
	for i := 0; i < worker; i++ {
		wait.Add(1)
		go func() {
			err := restoreWorkflow(backupCtx, wait)
			if err != nil {
				atomic.AddInt32(&backupCtx.fail, 1)
			}
		}()
	}
}

func waitUntilStop(backupCtx *BackupCtx, done chan struct{}) {
	stop := false
	for !stop {
		select {
		case <-done:
			stop = true
			backupCtx.stop = true
			break
		case <-time.After(3 * time.Second):
			break
		case <-backupCtx.stopEvent:
			stop = true
			backupCtx.stop = true
		}
	}
}

func backupAction(backupCtx *BackupCtx) error {
	params := make(map[string]interface{})
	_files, err := backupCtx.frontend.funcs["readdir_r"](backupCtx.frontend.handle, params)
	if err != nil {
		backupCtx.logger.Printf("[ERROR] pipeline [%s] readdir failed: %s\n", backupCtx.frontend.name, err.Error())
		return fmt.Errorf("frontend readdir failed: %s", err.Error())
	}
	files := _files.([]string)
	backupCtx.logger.Printf("[INFO] pipeline backup files: %s\n", strings.Join(files, ","))

	for _, file := range files {
		backupCtx.queue.AddTask(NewTask(file))
	}

	if backupCtx.workerCount > len(files) {
		backupCtx.workerCount = len(files)
	}

	backupCtx.status.Stage = "Running"
	backupCtx.status.AllFilesCount = len(files)
	backupCtx.status.CurrentNodeTasks = backupCtx.queue.Length()
	backupCtx.status.Upload = 0
	backupCtx.status.Download = 0

	backupCtx.logger.Printf("[INFO] pipeline start parallel backup, workerCount: %d\n", backupCtx.workerCount)

	var wait sync.WaitGroup
	backupdone := make(chan struct{})
	parallelBackupWorkflow(backupCtx, backupCtx.workerCount, &wait)
	go func() {
		wait.Wait()
		close(backupdone)
	}()
	waitUntilStop(backupCtx, backupdone)

	fail := atomic.LoadInt32(&backupCtx.fail)
	if fail > 0 {
		backupCtx.logger.Printf("[INFO] pipeline parallel backup failed, fail count: %d\n", fail)
		return errors.New("backup failed")
	}
	backupCtx.logger.Printf("[INFO] pipeline parallel backup success, prepare to exit\n")
	return nil
}

func restoreAction(backupCtx *BackupCtx) error {
	params := make(map[string]interface{})
	params["path"] = "/"
	_files, err := backupCtx.backend.funcs["readdir_r"](backupCtx.backend.handle, params)
	if err != nil {
		backupCtx.logger.Printf("[ERROR] pipeline [%s] readdir failed: %s\n", backupCtx.backend.name, err.Error())
		return fmt.Errorf("backend readdir failed")
	}
	files := _files.([]string)
	backupCtx.logger.Printf("[INFO] pipeline restore files: %+v", files)

	for _, file := range files {
		backupCtx.queue.AddTask(NewTask(file))
	}

	backupCtx.status.AllFilesCount = len(files)
	backupCtx.status.CurrentNodeTasks = backupCtx.queue.Length()
	backupCtx.status.Upload = 0
	backupCtx.status.Download = 0

	backupCtx.logger.Printf("[INFO] pipeline start parallel restore, workerCount: %d\n", backupCtx.workerCount)
	var wait sync.WaitGroup
	restoredone := make(chan struct{})
	backupCtx.stop = false
	parallelRestoreWorkflow(backupCtx, backupCtx.workerCount, &wait)
	go func() {
		wait.Wait()
		close(restoredone)
	}()
	waitUntilStop(backupCtx, restoredone)

	fail := atomic.LoadInt32(&backupCtx.fail)
	if fail > 0 {
		backupCtx.logger.Printf("[INFO] pipeline parallel restore failed, fail count: %d\n", fail)
		return errors.New("backup failed")
	}
	backupCtx.logger.Printf("[INFO] pipeline parallel restore success, prepare to exit\n")
	return nil
}

func restoreWorkflow(ctx *BackupCtx, wg *sync.WaitGroup) error {
	defer wg.Done()

	chunkBuf := make([]byte, ChunkBufSize)

	backendParam := make(map[string]interface{})
	frontendParam := make(map[string]interface{})
	for {
		if ctx.stop {
			break
		}
		task := ctx.queue.Get()
		if task == nil {
			break
		}

		// open backend file
		backendParam["name"] = task.name
		backendFile, err := ctx.backend.funcs["open"](ctx.backend.handle, backendParam)
		if err != nil {
			ctx.logger.Printf("[ERROR] pipeline %s open %s failed: %s\n", ctx.backend.name, task.name, err.Error())
			return err
		}
		backendReader, ok := backendFile.(io.Reader)
		if !ok {
			return errors.New("invalid backendFile")
		}

		// create database file
		frontendParam["name"] = task.name
		frontendFile, err := ctx.frontend.funcs["create"](ctx.frontend.handle, frontendParam)
		if err != nil {
			ctx.logger.Printf("[ERROR] pipeline %s create file %s failed: %s\n", ctx.frontend.name, task.name, err.Error())
			return err
		}
		frontendFileWriter, ok := frontendFile.(io.Writer)
		if !ok {
			return errors.New("invalid frontendFile")
		}

		if ctx.compress {
			// decompress
			decompress := lz4.NewReader(backendReader)
			if err != nil {
				ctx.logger.Printf("[ERROR] pipeline compress mode enable, lz4 decompress failed: %s\n", err.Error())
				return err
			}

			_, _, err = CopyBufferAlign(frontendFileWriter, decompress, chunkBuf)
			if err != nil {
				ctx.logger.Printf("[ERROR] pipeline compress mode enable, lz4 compress failed: %s\n", err.Error())
				return err
			}
		} else {
			_, _, err = CopyBufferAlign(frontendFileWriter, backendReader, chunkBuf)
			if err != nil {
				ctx.logger.Printf("[ERROR] pipeline normal mode, copy file failed: %s\n", err.Error())
				return err
			}
		}

		// close files
		backendParam["file"] = backendFile
		ctx.backend.funcs["close"](ctx.backend.handle, backendParam)

		frontendParam["file"] = frontendFile
		ctx.frontend.funcs["close"](ctx.frontend.handle, frontendParam)

		downloaded := atomic.AddInt32(&ctx.status.Download, 1)

		ctx.logger.Printf("[INFO] pipeline download file %s, total count: %d, current count: %d\n", task.name, ctx.status.AllFilesCount, downloaded)
	}

	return nil
}

func backupWorkflow(ctx *BackupCtx, wg *sync.WaitGroup) error {
	defer wg.Done()

	chunkBuf := make([]byte, ChunkBufSize)

	backendParam := make(map[string]interface{})
	frontendParam := make(map[string]interface{})
	for {
		if ctx.stop {
			break
		}
		task := ctx.queue.Get()
		if task == nil {
			break
		}

		ctx.status.Stage = "Running"

		// open database file
		frontendParam["name"] = task.name
		frontendFile, err := ctx.frontend.funcs["open"](ctx.frontend.handle, frontendParam)
		if err != nil {
			ctx.logger.Printf("[ERROR] pipeline %s open %s failed: %s\n", ctx.frontend.name, task.name, err.Error())
			return err
		}
		frontendFileReader, ok := frontendFile.(io.Reader)
		if !ok {
			return errors.New("invalid frontendFile")
		}

		// create backend file
		backendParam["name"] = task.name
		if ctx.force {
			ctx.logger.Printf("[ERROR] pipeline [Force mode], %s unlink file %s\n", ctx.backend.name, task.name)
			_, _ = ctx.backend.funcs["unlink"](ctx.backend.handle, backendParam)
		}
		backendFile, err := ctx.backend.funcs["create"](ctx.backend.handle, backendParam)
		if err != nil {
			ctx.logger.Printf("[ERROR] pipeline %s create file %s failed: %s\n", ctx.backend.name, task.name, err.Error())
			return err
		}

		backendWriter, ok := backendFile.(io.Writer)
		if !ok {
			return errors.New("invalid backendFile")
		}

		if ctx.compress {
			compress := lz4.NewWriter(backendWriter)
			if err != nil {
				ctx.logger.Printf("[ERROR] pipeline compress mode enable, lz4 compress failed: %s\n", err.Error())
				return err
			}

			_, _, err = CopyBufferAlign(compress, frontendFileReader, chunkBuf)
			if err != nil {
				ctx.logger.Printf("[ERROR] pipeline compress mode, copy file failed: %s\n", err.Error())
				return err
			}
			compress.Close()
		} else {
			_, _, err = CopyBufferAlign(backendWriter, frontendFileReader, chunkBuf)
			if err != nil {
				ctx.logger.Printf("[ERROR] pipeline normal mode, copy file failed: %s\n", err.Error())
				return err
			}
		}

		backendParam["file"] = backendFile
		ctx.backend.funcs["close"](ctx.backend.handle, backendParam)
		frontendParam["file"] = frontendFile
		ctx.frontend.funcs["close"](ctx.frontend.handle, frontendParam)

		uploaded := 0

		ctx.mutex.Lock()
		ctx.status.Upload += 1
		uploaded = ctx.status.Upload
		//ctx.status.DoneFiles = append(ctx.status.DoneFiles, task.name)
		//sort.Strings(ctx.status.DoneFiles)
		ctx.mutex.Unlock()

		ctx.logger.Printf("[INFO] pipeline upload file %s, total count: %d, current count: %d\n", task.name, ctx.status.AllFilesCount, uploaded)
	}
	return nil
}

package main

/*
#include <waldump.h>
*/
import "C"
import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/pierrec/lz4"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
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
		return nil, errors.New(fmt.Sprintf("ctx.worker must in [1, %d]", MaxWorkerCount))
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
		return nil, errors.New("ctx.imports must be func(inteface{},map[string]interface{}(interface{}, error)")
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
	backupCtx.action = backupCtx.conf.Action
	backupCtx.hostip = backupCtx.conf.HostIP
	backupCtx.enableBlockBackup = backupCtx.conf.EnableBlockBackup
	backupCtx.enableEncryption = backupCtx.conf.EnableEncryption
	backupCtx.encryptionPassword = backupCtx.conf.EncryptionPassword

	if backupCtx.encryptionPassword == "" {
		backupCtx.encryptionPassword = DefaultEncryptionPassword
	}

	if backupCtx.conf.BlockBackupDir != "" {
		backupCtx.blockBackupDir = backupCtx.conf.BlockBackupDir
	} else {
		backupCtx.blockBackupDir = DefaultBlockBackupDir
	}

	backupCtx.logdir = "pg_wal"
	backupCtx.excludeDirs = []string{"pg_wal"}
	backupCtx.excludeFiles = []string{}

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

	if backupCtx == nil {
		err = errors.New("ctx must not be nil")
		return err
	}

	CapturePanic(backupCtx)

	if backupCtx.stop {
		err = errors.New("backupjob in current plugin recieve stop yet")
		return err
	}

	action, extraInfo, err := getActionFromTask(backupCtx, param)
	if err != nil {
		return err
	}
	if action == "limit" {
		backupCtx.bpsController.maxBps = int64(extraInfo.MaxBackupSpeed) * 1024 * 1024
		return nil
	}
	if action == "updatetopo" {
		return updateTopology(backupCtx, extraInfo.Master)
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
		backupCtx.logger.Printf("[ERROR] pipeline init task failed, exit now\n")
		backupCtx.status.Error = err.Error()
		backupCtx.status.Stage = "Failed"
		NotifyManagerCallback(backupCtx, backupCtx.status.Stage)
		return err
	}

	if backupCtx.job.WorkerCount != 0 {
		backupCtx.workerCount = backupCtx.job.WorkerCount
	}

	// override action
	if backupCtx.job.Action != "" {
		if backupCtx.lastAction == backupCtx.job.Action {
			err = errors.New("increpipeline ctx receive same action: " + backupCtx.lastAction + " Please check the new task.")
			backupCtx.logger.Printf("[ERROR] %s\n", err.Error())
			return err
		}
		backupCtx.status.Action = backupCtx.job.Action
		backupCtx.conf.Action = backupCtx.job.Action
		backupCtx.action = backupCtx.job.Action
		if backupCtx.job.Location != "" {
			backupCtx.status.Location = backupCtx.job.Location
		}

		backupCtx.lastAction = backupCtx.action
		backupCtx.enableEncryption = backupCtx.job.EnableEncryption
		if backupCtx.job.EncryptionPassword != "" {
			backupCtx.encryptionPassword = backupCtx.job.EncryptionPassword
		}
	}

	if backupCtx.action == "stop" {
		/*
		 * Do not callback Stop status or set stage here, since some workflows may still backup wals and callback Running status, and it will make reciever confused.
		 */
		backupCtx.stop = true
		if backupCtx.bkdbinfo == nil { // backup_ctl restart, we need to init db and update backup status to Stop
			backupCtx.status.Stage = "Stop"
			if backupCtx.status.Location == "" {
				backupCtx.status.Location = "default"
			}
			err = repairIncrementalStatusToMultiRemoteDB(backupCtx)
			if err != nil {
				backupCtx.logger.Printf("[ERROR] repair Incremental Status To MultiRemoteDB failed: %s\n", err.Error())
				return err
			}
		}
		// NotifyManagerCallback(backupCtx, "Stop")
		err = nil
		return err
	}

	defer func() {
		if !backupCtx.cmdline && backupCtx.job.Action == "restore" {
			var file *os.File
			if err == nil {
				file, err = os.Create("icrefinish")
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
	backupCtx.status.EndTime = backupCtx.status.StartTime
	backupCtx.status.Backend = backupCtx.job.Backend
	backupCtx.status.Type = "Incremental"

	backupCtx.status.BackupID = backupCtx.job.BackupID
	backupCtx.status.BackupJobID = backupCtx.job.BackupJobID
	backupCtx.status.InstanceID = backupCtx.job.InstanceID
	backupCtx.status.Callback = backupCtx.job.CallbackURL
	backupCtx.status.DBSCallbackURL = backupCtx.job.DBSCallbackURL
	backupCtx.status.BackupJobID = backupCtx.job.BackupJobID

	if backupCtx.job.BackupAccount.Endpoint != "" {
		backupCtx.conf.PgDBConf.Endpoint = strings.Split(backupCtx.job.BackupAccount.Endpoint, ",")[0]
		backupCtx.conf.PgDBConf.Port = backupCtx.job.BackupAccount.Port
		backupCtx.conf.PgDBConf.Username = backupCtx.job.BackupAccount.User
		backupCtx.conf.PgDBConf.Password = backupCtx.job.BackupAccount.Password
	}
	backupCtx.status.PgDBConf = backupCtx.conf.PgDBConf

	frontEnd := backupCtx.conf.Frontend
	if backupCtx.job.Frontend != "" {
		frontEnd = backupCtx.job.Frontend
	}
	err = initFunction(backupCtx, frontEnd, "Frontend")
	if err != nil {
		backupCtx.status.Stage = "Failed"
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
		backupCtx.status.Stage = "Failed"
		backupCtx.status.Error = err.Error()
		NotifyManagerCallback(backupCtx, "Failed")
		return err
	}

	backupCtx.status.Stage = "Preparing"
	NotifyManagerCallback(backupCtx, backupCtx.status.Stage)

	if backupCtx.action == "backup" {
		err, _ = initDbs(backupCtx)
		if err != nil {
			backupCtx.logger.Printf("[ERROR] pgpipeline init db failed, exit now\n")
			backupCtx.status.Stage = "Failed"
			backupCtx.status.Error = err.Error()
			NotifyManagerCallback(backupCtx, backupCtx.status.Stage)
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
		if backupCtx.enableBlockBackup {
			err = initBlockBackupFlag(backupCtx.bkdbinfo)
			if err != nil {
				backupCtx.logger.Printf("[ERROR] pgpipeline initBlockBackupFlag failed, exit now\n")
				backupCtx.status.Error = err.Error()
				NotifyManagerCallback(backupCtx, "Failed")
				return err
			}
		}
	}

	backupCtx.logger.Printf("[INFO] increpipeline start [report] goroutine, report addr: %s\n", backupCtx.manager)
	backupCtx.status.Stage = "Running"
	go func() {
		lastReportWalName := ""
		lastReportWalTime := int64(0)

		stop := false
		for !stop {
			if backupCtx.stop {
				backupCtx.logger.Printf("[INFO] increpipeline %s stop, [report] goroutine exit\n", backupCtx.action)
				stop = true
			}

			if backupCtx.action == "backup" {
				wal, _, e := getCurrentWalFile(backupCtx)
				if e != nil {
					backupCtx.logger.Printf("[ERROR] get current wal file failed: %s\n", e.Error())
				} else {
					backupCtx.status.UsingLogFile.FileName = wal
				}
			}

			// backupCtx.logger.Printf("[INFO] increpipeline [report] %s cmd status: %s\n", backupCtx.action, backupCtx.status.Stage)
			backupCtx.mutex.Lock()
			NotifyManagerCallback(backupCtx, backupCtx.status.Stage)
			backupCtx.mutex.Unlock()

			time.Sleep(5 * time.Second)

			backupCtx.mutex.Lock()
			if lastReportWalName != backupCtx.status.CurLogFile.FileName {
				lastReportWalName = backupCtx.status.CurLogFile.FileName
				lastReportWalTime = time.Now().Unix()
			}

			if lastReportWalName != "" && time.Now().Unix() - lastReportWalTime > 60 {
				backupCtx.status.CurLogFile.FileName = ""
				lastReportWalName = ""
			}
			backupCtx.mutex.Unlock()
		}
	}()

	if backupCtx.action == "backup" && backupCtx.conf.SwitchWalPeroid > 0 {
		SwitchWalInPeroid(backupCtx)
	}

	backupCtx.logger.Printf("[INFO] increpipeline start bpscontroller\n")
	backupCtx.bpsController = &BPSController{}
	backupCtx.bpsController.init(backupCtx.job.MaxBackupSpeed)
	go func() {
		stop := false
		for !stop {
			if backupCtx.stop {
				backupCtx.logger.Printf("[INFO] increpipeline %s stop, [report] goroutine exit\n", backupCtx.action)
				stop = true
			}
			backupCtx.bpsController.adjustCore()
			time.Sleep(1000 * time.Millisecond)
		}
	}()

	// go func() {
	//     stop := false
	//     var lastBytes,curBps int64
	//     curBps = 0
	//     lastBytes = 0
	//     for !stop {
	//         if backupCtx.stop {
	//             backupCtx.logger.Printf("[INFO] increpipeline %s stop, [report] goroutine exit\n", backupCtx.action)
	//             stop = true
	//         }
	//         if lastBytes == 0 {
	//             lastBytes = backupCtx.bpsController.curBytes
	//         } else {
	//             curBytes := backupCtx.bpsController.curBytes
	//             curBps = (curBytes - lastBytes) / (5 * 1048576)
	//             lastBytes = curBytes
	//         }
	//         backupCtx.logger.Printf("[INFO] increpipeline curent bps: %d MB/s\n", curBps)
	//         time.Sleep(5 * time.Second)
	//     }
	// }()

	backupCtx.logger.Printf("[INFO] increpipeline init dependence conf begin...\n")
	err = initDependence(backupCtx, extern)
	if err != nil {
		backupCtx.logger.Printf("[ERROR] increpipeline init dependence failed: %s\n", err.Error())
		return err
	}
	backupCtx.logger.Printf("[INFO] increpipeline init dependence success\n")

	backupCtx.conf.MaxWalsBuffer = 1024
	backupCtx.wallogs = make(chan LogFileInfo, backupCtx.conf.MaxWalsBuffer)
	backupCtx.logger.Printf("[INFO] increpipeline MaxWalsBuffer set to %d\n", backupCtx.conf.MaxWalsBuffer)

	if backupCtx.cmdline {
		backupCtx.status.InstanceID = backupCtx.conf.InstanceID
		backupCtx.status.BackupID = backupCtx.conf.BackupID
		backupCtx.status.BackupJobID = "default"
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

	action = backupCtx.action
	if action == "backup" {
		backupCtx.logger.Printf("[INFO] increpipeline do backup now\n")
		// compress info need to send to backend for restore
		err = backupAction(backupCtx)
	} else if action == "restore" {
		backupCtx.logger.Printf("[INFO] increpipeline do restore now\n")
		err = restoreAction(backupCtx)
	} else {
		backupCtx.logger.Printf("[ERROR] increpipeline invalid action: %s\n", backupCtx.action)
	}

	if err != nil {
		backupCtx.logger.Printf("[ERROR] increpipeline %s failed: %s\n", backupCtx.action, err.Error())
		backupCtx.status.Error = err.Error()
		backupCtx.status.Stage = "Failed"
		NotifyManagerCallback(backupCtx, backupCtx.status.Stage)
		if backupCtx.cmdline {
			os.Exit(1)
		}
	}

	finiDependence(backupCtx)
	backupCtx.logger.Printf("[INFO] pipelinefini dependence plugin\n")

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
		}
	}
	return nil
}

func SwitchWalInPeroid(backupCtx *BackupCtx) {
	go func() {
		lastWal := ""
		stop := false
		for !stop {
			if backupCtx.stop {
				backupCtx.logger.Printf("[INFO] goroutine of switch wal in peroid exit\n")
				stop = true
			}

			curWal, _, e := getCurrentWalFile(backupCtx)
			if e != nil {
				backupCtx.logger.Printf("[ERROR] get current wal file failed: %s\n", e.Error())
			} else {
				if lastWal == "" {
					lastWal = curWal
				} else if lastWal == curWal {
					backupCtx.logger.Printf("[INFO] wal is still %s after %d seconds, try switch wal\n", curWal, backupCtx.conf.SwitchWalPeroid)
					err := pgSwitchWal(backupCtx.bkdbinfo)
					if err != nil {
						backupCtx.logger.Printf("[ERROR] switch wal failed: %s", err.Error())
					} else {
						backupCtx.logger.Printf("[INFO] switch wal done")
						time.Sleep(5 * time.Second)
						newWal, _, e := getCurrentWalFile(backupCtx)
						if e != nil {
							backupCtx.logger.Printf("[ERROR] get current wal file failed: %s\n", e.Error())
						} else {
							lastWal = newWal
							backupCtx.logger.Printf("[INFO] wal change from %s to %s", curWal, newWal)
						}
					}
				} else {
					lastWal = curWal
				}
			}

			time.Sleep(time.Duration(backupCtx.conf.SwitchWalPeroid) * time.Second)
		}
	}()
}

func _backupWorkflowWithoutArchive(ctx *BackupCtx) (int, error) {
	done := false
	prepareStop := false
	count := 0

	for !done {
		if ctx.stop {
			prepareStop = true
			/*
			 * stop backup workflow as soon as possible if stop flag is true in daemon mode.
			 */
			if !ctx.cmdline {
				done = true
			}
		}

		var task *Task
		if ctx != nil && ctx.queue != nil {
			task = ctx.queue.Get()
		} else {
			ctx.logger.Printf("[ERROR] can not get task since ctx is nil or queue is nil\n")
		}

		if task == nil {
			if prepareStop {
				done = true
			}
			time.Sleep(2 * time.Second)
			continue
		}

		switch task.mode {
		case WALLSN:
			err := _backupWalLSN(ctx, task)
			if err != nil {
				record.Delete(task.name)
				ctx.logger.Printf("[ERROR] backup wal lsn failed: %s\n", task.name)
				if ctx.cmdline {
					return count, err
				}
			}
		case WholeWALSeg:
			err := _backupWholeWalSeg(ctx, task)
			if err != nil {
				record.Delete(task.name)
				ctx.logger.Printf("[ERROR] backup whole wal %s failed: %s\n", task.name, err.Error())
				if ctx.cmdline {
					return count, err
				}
			}
		}

		ctx.logger.Printf("[INFO] backup wal file [%s] finished. \n", task.name)

		count++
	}
	return count, nil
}

func updateTopology(ctx *BackupCtx, master string) error {
	if ctx.bkdbinfo == nil {
		return nil
	}

	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()

	closeDB(ctx.bkdbinfo)
	ctx.bkdbinfo = nil

	masterSplit := strings.Split(master, ":")
	if len(masterSplit) != 2 {
		return errors.New("master is not ip:port format:" + master)
	}

	ctx.conf.PgDBConf.Endpoint = masterSplit[0]
	ctx.conf.PgDBConf.Port = masterSplit[1]

	ctx.logger.Printf("[INFO] ip set to %s, port set to %s\n", ctx.conf.PgDBConf.Endpoint, ctx.conf.PgDBConf.Port)

	err, _ := initDbs(ctx)

	if err != nil {
		ctx.logger.Printf("[ERROR] updateTopology failed: %s\n", err.Error())
	}

	return err
}

func repairIncrementalStatusToMultiRemoteDB(ctx *BackupCtx) error {
	if ctx.job.BackupAccount.Endpoint != "" {
		ctx.conf.PgDBConf.Endpoint = strings.Split(ctx.job.BackupAccount.Endpoint, ",")[0]
		ctx.conf.PgDBConf.Port = ctx.job.BackupAccount.Port
		ctx.conf.PgDBConf.Username = ctx.job.BackupAccount.User
		ctx.conf.PgDBConf.Password = ctx.job.BackupAccount.Password
	}

	ctx.status.BackupID = ctx.job.BackupID
	ctx.status.BackupJobID = ctx.job.BackupJobID
	ctx.status.InstanceID = ctx.job.InstanceID

	err, wc := initDbs(ctx)
	if err != nil && wc == 0 {
		ctx.logger.Printf("[ERROR] increpipeline init db failed, exit now\n")
		ctx.status.Stage = "Failed"
		ctx.status.Error = err.Error()
		NotifyManagerCallback(ctx, ctx.status.Stage)
		return err
	}

	defer func() {
		closeDB(ctx.bkdbinfo)
		closeDB(ctx.mtdbinfo)
	}()

	err = updateIncrementalStatusToMultiRemoteDB(ctx, true)
	if err != nil {
		ctx.logger.Printf("[ERROR] increpipeline plugin update backup info to remote db failed: %s", err.Error())
		return err
	}

	ctx.logger.Printf("[INFO] increpipeline repair Incremental Status To MultiRemoteDB done")

	return nil
}

/*
func CopyN(dst io.Writer, src io.Reader, buf []byte, n int64) (int64, error) {
    var left, written int64
    var err error
    left = n
    for {
        if left < FileBufSize {
            if left < MinBufSize {
                wsize, err := io.CopyN(dst, src, left)
                written = written + wsize
                return written, err
            } else {
                roundsize := (left / MinBufSize) * MinBufSize
                tmpBuf := make([]byte, roundsize)
                rsize, er := src.Read(tmpBuf)
                if rsize > 0 {
                    blockbuf := tmpBuf[0:rsize]
                    wsize, ew := dst.Write(blockbuf)
                    if ew != nil {
                        err = ew
                        break
                    }
                    written = written + int64(wsize)
                    left = left - int64(rsize)
                }
                if er != nil {
                    if er != io.EOF {
                        err = er
                    }
                    break
                }
                continue
            }
        }
        rsize, er := src.Read(buf)
        if rsize > 0 {
            blockbuf := buf[0:rsize]
            wsize, ew := dst.Write(blockbuf)
            if ew != nil {
                err = ew
                break
            }
            written = written + int64(wsize)
            left = left - int64(rsize)
        }
        if er != nil {
            if er != io.EOF {
                err = er
            }
            break
        }
    }
    return written, err
}
*/

func ReadFile(ctx *BackupCtx, file string) ([]byte, error) {
	var originBuf []byte

	metaFile, err := OpenFile(ctx.backend, file)
	if err != nil {
		ctx.logger.Printf("[ERROR] open meta file failed. err[%s]\n", err.Error())
		return nil, err
	}

	metaReader, ok := metaFile.(io.Reader)
	if !ok {
		err = errors.New("invalid metaFile")
		return nil, err
	}

	buf := make([]byte, 1024*1024)
	for {
		nr, er := metaReader.Read(buf)
		if nr > 0 {
			originBuf = append(originBuf, buf[0:nr]...)
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	Close(ctx.backend, metaFile)
	if err != nil {
		ctx.logger.Printf("[ERROR] read origin meta file failed. err[%s]\n", err.Error())
		return nil, err
	}

	return originBuf, nil
}

func calculateStage(ctx *BackupCtx) (int, []byte, error) {
	var err error
	var originBuf []byte

	var ismetaExist, istmpExist, isreadyExist, iserrExist bool

	ret, err := ExistFile(ctx.backend, "wals.pg_o_backup_meta.err")
	if err != nil {
		ctx.logger.Printf("[ERROR] exist %s failed: %s\n", "wals.pg_o_backup_meta.err", err.Error())
		return -1, originBuf, err
	}
	iserrExist = ret.(bool)

	ret, err = ExistFile(ctx.backend, "wals.pg_o_backup_meta")
	if err != nil {
		ctx.logger.Printf("[ERROR] exist %s failed: %s\n", "wals.pg_o_backup_meta", err.Error())
		return -1, originBuf, err
	}
	ismetaExist = ret.(bool)

	ret, err = ExistFile(ctx.backend, "wals.pg_o_backup_meta.tmp")
	if err != nil {
		ctx.logger.Printf("[ERROR] exist %s failed: %s\n", "wals.pg_o_backup_meta", err.Error())
		return -1, originBuf, err
	}
	istmpExist = ret.(bool)

	ret, err = ExistFile(ctx.backend, "wals.pg_o_backup_meta.tmp.ready")
	if err != nil {
		ctx.logger.Printf("[ERROR] exist %s failed: %s\n", "wals.pg_o_backup_meta", err.Error())
		return -1, originBuf, err
	}
	isreadyExist = ret.(bool)

	/*
	 * err exist at stage 0, delete meta if exist and goto stage 0 to create origin meta again
	 */
	if iserrExist {
		if ismetaExist {
			_, err := UnlinkFile(ctx.backend, "wals.pg_o_backup_meta")
			if err != nil {
				ctx.logger.Printf("[ERROR] unlink file %s failed: %s\n", "wals.pg_o_backup_meta", err.Error())
				return -1, originBuf, err
			}
		}
		_, err := UnlinkFile(ctx.backend, "wals.pg_o_backup_meta.err")
		if err != nil {
			ctx.logger.Printf("[ERROR] unlink file %s failed: %s\n", "wals.pg_o_backup_meta.err", err.Error())
			return -1, originBuf, err
		}
		return 0, originBuf, nil
	}

	/*
	 * no meta file or tmp exist, goto stage 0 to create origin meta
	 */
	if !ismetaExist && !istmpExist {
		return 0, originBuf, nil
	}

	/*
	 * origin meta exist and tmp is not exist, goto normal stage 1
	 */
	if ismetaExist && !istmpExist {
		return 1, originBuf, nil
	}

	/*
	 * tmp exist and origin meta is not exist, goto stage 4, recovery origin meta buf from tmp
	 */
	if !ismetaExist && istmpExist {
		originBuf, err = ReadFile(ctx, "wals.pg_o_backup_meta.tmp")
		return 4, originBuf, err
	}

	if ismetaExist && istmpExist {
		if isreadyExist {
			/*
			 * ready file of tmp exist, delete meta and recovery origin meta buf from tmp since tmp is complete and meta may be not complete
			 */
			_, err := UnlinkFile(ctx.backend, "wals.pg_o_backup_meta")
			if err != nil {
				ctx.logger.Printf("[ERROR] unlink file %s failed: %s\n", "wals.pg_o_backup_meta", err.Error())
				return -1, originBuf, err
			}
			originBuf, err = ReadFile(ctx, "wals.pg_o_backup_meta.tmp")
			return 4, originBuf, err
		} else {
			/*
			 * ready file of tmp not exist, delete tmp and recovery origin meta buf from origin meta since meta is complete and tmp may be not complete
			 */
			_, err := UnlinkFile(ctx.backend, "wals.pg_o_backup_meta.tmp")
			if err != nil {
				ctx.logger.Printf("[ERROR] unlink file %s failed: %s\n", "wals.pg_o_backup_meta.tmp", err.Error())
				return -1, originBuf, err
			}
			return 1, originBuf, nil
		}
	}

	return -1, originBuf, errors.New("reach unkown stage")
}

/*
 * To handle the exception, we divide the process of update wals meta as following stages and process it using a state machine.
 * updateWalsMeta will goto stage 0 at first time, if everything goes fine, the following calling will goto stage 1 and exit at stage 4,
 * then if something goes wrong between these steps such as excpetion, the state machine will calculate the proper stage to go on.
 * stage 0: create origin meta
 * stage 1: copy origin meta to tmp -> stage 2: create tmp.ready -> stage 3: delete origin meta -> stage 4:update new meta(create new meta-> delete tmp -> delete tmp.ready)
 */
func updateWalsMeta(ctx *BackupCtx, newLogFiles []LogFileInfo) error {
	tb := time.Now()

	var originBuf []byte
	var backendWriter io.Writer
	var backendFile, readyFile, ret interface{}
	var ok bool
	var wz int

	stage, originBuf, err := calculateStage(ctx)
	if err != nil {
		ctx.logger.Printf("[ERROR] calculate stage of update wals's meta failed: %s\n", err.Error())
		return err
	}

	switch stage {
	case 0:
		goto stage0
	case 1:
		goto stage1
		break
	case 2:
		goto stage2
		break
	case 3:
		goto stage3
		break
	case 4:
		goto stage4
		break
	default:
		return errors.New("no such stage")
	}

stage0:
	err = createWalsMeta(ctx, newLogFiles)
	if err != nil {
		ctx.logger.Printf("[ERROR] create wals's meta file %s failed: %s\n", "wals.pg_o_backup_meta", err.Error())
		/*
		* if error occur when create meta, we should unlink it if exist and return error to trigger creating meta again
		 */
		ret, err = ExistFile(ctx.backend, "wals.pg_o_backup_meta")
		if err != nil {
			ctx.logger.Printf("[ERROR] exist file %s failed: %s\n", "wals.pg_o_backup_meta", err.Error())
			return err
		}
		if ret.(bool) {
			_, err := UnlinkFile(ctx.backend, "wals.pg_o_backup_meta")
			if err != nil {
				ctx.logger.Printf("[ERROR] unlink file %s failed: %s\n", "wals.pg_o_backup_meta", err.Error())
				return err
			}
		}
	} else {
		_, err := UnlinkFile(ctx.backend, "wals.pg_o_backup_meta.err")
		if err != nil {
			ctx.logger.Printf("[ERROR] unlink file %s failed: %s\n", "wals.pg_o_backup_meta.err", err.Error())
			return err
		}
	}
	return err

stage1:
	/*
	 * copy origin meta to tmp
	 */
	originBuf, err = ReadFile(ctx, "wals.pg_o_backup_meta")
	if err != nil {
		ctx.logger.Printf("[ERROR] read backend file wals.pg_o_backup_meta failed, err[%s]\n", err.Error())
		return err
	}

	backendFile, err = CreateFile(ctx.backend, "wals.pg_o_backup_meta.tmp")
	if err != nil {
		ctx.logger.Printf("[ERROR] create backend file wals.pg_o_backup_meta.tmp failed, err[%s]\n", err.Error())
		return err
	}

	backendWriter, ok = backendFile.(io.Writer)
	if !ok {
		Close(ctx.backend, backendFile)
		err = errors.New("invalid backendFile")
		return err
	}

	wz, err = backendWriter.Write(originBuf)
	if err != nil {
		Close(ctx.backend, backendFile)
		return err
	}

	if wz != len(originBuf) {
		ctx.logger.Printf("[ERROR] write backend file wals.pg_o_backup_meta.tmp failed, len buf: %d, write sz: %d\n", len(originBuf), wz)
		Close(ctx.backend, backendFile)
		return errors.New("write backend file wals.pg_o_backup_meta.tmp failed")
	}

	Close(ctx.backend, backendFile)

stage2:
	/*
	 * create tmp.ready
	 */
	readyFile, err = CreateFile(ctx.backend, "wals.pg_o_backup_meta.tmp.ready")
	if err != nil {
		ctx.logger.Printf("[ERROR] create backend file wals.pg_o_backup_meta.tmp.ready failed, err[%s]\n", err.Error())
		return err
	}
	Close(ctx.backend, readyFile)

stage3:
	/*
	 * delete origin meta
	 */
	_, err = UnlinkFile(ctx.backend, "wals.pg_o_backup_meta")
	if err != nil {
		ctx.logger.Printf("[ERROR] fs[%s] unlink[%s] failed, err[%s]\n", ctx.backend.name, "wals.pg_o_backup_meta", err.Error())
		return err
	}

stage4:
	/*
	 * update new meta(create new meta-> delete tmp -> delete tmp.ready)
	 */
	err = updateWalsMetaCore(ctx, newLogFiles, originBuf)
	if err != nil {
		ctx.logger.Printf("[ERROR] update wals's meta %s failed: %s\n", "wals.pg_o_backup_meta", err.Error())
		return err
	}

	te := time.Now()
	ctx.logger.Printf("[INFO] update meta consume time: %s\n", te.Sub(tb))

	return nil
}

func updateWalsMetaCore(ctx *BackupCtx, newLogFiles []LogFileInfo, originBuf []byte) error {
	backendFile, err := CreateFile(ctx.backend, "wals.pg_o_backup_meta")
	if err != nil {
		ctx.logger.Printf("[ERROR] create backend file wals.pg_o_backup_meta failed, err[%s]\n", err.Error())
		return err
	}

	isClose := false
	defer func() {
		if !isClose {
			Close(ctx.backend, backendFile)
		}
	}()

	backendWriter, ok := backendFile.(io.Writer)
	if !ok {
		err = errors.New("invalid backendFile")
		return err
	}

	i := 0
	for ; i < len(originBuf); i++ {
		if originBuf[i] == '\n' {
			break
		}
	}
	n, err := strconv.Atoi(string(originBuf[0:i]))
	if err != nil {
		ctx.logger.Printf("[ERROR] convert first line of wals.pg_o_backup_meta failed, err[%s]\n", err.Error())
		return err
	}

	totalwz := 0
	newn := len(newLogFiles)
	if newn >= ctx.conf.MaxWalsRecord {
		newn = ctx.conf.MaxWalsRecord
		wz, err := backendWriter.Write([]byte(strconv.Itoa(newn) + "\n"))
		if err != nil {
			return err
		}
		totalwz = totalwz + wz
		for i := 0; i < newn; i++ {
			wal := newLogFiles[i]
			record := genWalRecord(wal)
			buf := []byte(record)
			wz, ew := backendWriter.Write(buf)
			if ew != nil {
				return ew
			}
			totalwz = totalwz + wz
		}
	} else {
		res := ctx.conf.MaxWalsRecord - newn
		var bindex, eindex, boffset, eoffset int
		eindex = n
		if res >= n {
			bindex = 1
		} else {
			bindex = n - res + 1
		}

		index := 0
		for ; i < len(originBuf); i++ {
			if originBuf[i] == '\n' {
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
			return errors.New("[ERROR] parse wal meta failed, boffset is larger than eoffset")
		}

		wz, err := backendWriter.Write([]byte(strconv.Itoa(newn+eindex-bindex+1) + "\n"))
		if err != nil {
			return err
		}
		totalwz = totalwz + wz
		wz, err = backendWriter.Write(originBuf[boffset : eoffset+1])
		if err != nil {
			return err
		}
		totalwz = totalwz + wz

		for i := 0; i < newn; i++ {
			wal := newLogFiles[i]
			record := genWalRecord(wal)
			buf := []byte(record)
			wz, ew := backendWriter.Write(buf)
			if ew != nil {
				return ew
			}
			totalwz = totalwz + wz
		}
	}

	_, err = Close(ctx.backend, backendFile)
	isClose = true
	if err != nil {
		ctx.logger.Printf("[ERROR] close file %s failed. err[%s]\n", "wals.pg_o_backup_meta", err.Error())
		return err
	}

	ctx.logger.Printf("[INFO] after update wal, wz of file %s is %d, while len of oribuf is %d\n", "wals.pg_o_backup_meta", totalwz, len(originBuf))

	ret, err := ExistFile(ctx.backend, "wals.pg_o_backup_meta.tmp")
	if err != nil {
		ctx.logger.Printf("[ERROR] exist file %s failed: %s\n", "wals.pg_o_backup_meta.tmp", err.Error())
		return err
	}
	if ret.(bool) {
		_, err = UnlinkFile(ctx.backend, "wals.pg_o_backup_meta.tmp")
		if err != nil {
			ctx.logger.Printf("[ERROR] fs[%s] unlink[%s] failed, err[%s]\n", ctx.backend.name, "wals.pg_o_backup_meta.tmp", err.Error())
			return err
		}
	} else {
		ctx.logger.Printf("[ERROR] not found wals.pg_o_backup_meta.tmp in stage 4, please check the status of wals.pg_o_backup_meta\n")
		return errors.New("not found wals.pg_o_backup_meta.tmp in stage 4, please check the status of wals.pg_o_backup_meta")
	}

	ret, err = ExistFile(ctx.backend, "wals.pg_o_backup_meta.tmp.ready")
	if err != nil {
		ctx.logger.Printf("[ERROR] exist file %s failed: %s\n", "wals.pg_o_backup_meta.tmp.ready", err.Error())
		return err
	}
	if ret.(bool) {
		_, err := UnlinkFile(ctx.backend, "wals.pg_o_backup_meta.tmp.ready")
		if err != nil {
			ctx.logger.Printf("[ERROR] unlink file %s failed: %s\n", "wals.pg_o_backup_meta.tmp.ready", err.Error())
			return err
		}
	}

	return nil
}

func createWalsMeta(ctx *BackupCtx, newLogFiles []LogFileInfo) error {
	n := len(newLogFiles)
	if n == 0 {
		return nil
	}

	errFile, err := CreateFile(ctx.backend, "wals.pg_o_backup_meta.err")
	if err != nil {
		ctx.logger.Printf("[ERROR] create backend file wals.pg_o_backup_meta.err failed, err[%s]\n", err.Error())
		return err
	}
	defer Close(ctx.backend, errFile)

	backendFile, err := CreateFile(ctx.backend, "wals.pg_o_backup_meta")
	if err != nil {
		ctx.logger.Printf("[ERROR] create backend file wals.pg_o_backup_meta failed, err[%s]\n", err.Error())
		return err
	}
	defer Close(ctx.backend, backendFile)

	backendWriter, ok := backendFile.(io.Writer)
	if !ok {
		err = errors.New("invalid backendFile")
		return err
	}

	if n > ctx.conf.MaxWalsRecord {
		n = ctx.conf.MaxWalsRecord
	}

	// TODO sort by lsn if larger than MaxWalsRecord
	_, err = backendWriter.Write([]byte(strconv.Itoa(n) + "\n"))
	if err != nil {
		ctx.logger.Printf("[ERROR] write backend file wals.pg_o_backup_meta failed, err[%s]\n", err.Error())
		return err
	}
	for i := 0; i < n; i++ {
		wal := newLogFiles[i]
		record := genWalRecord(wal)
		buf := []byte(record)
		_, ew := backendWriter.Write(buf)
		if ew != nil {
			ctx.logger.Printf("[ERROR] write backend file wals.pg_o_backup_meta failed, err[%s]\n", err.Error())
			return ew
		}
	}

	return nil
}

func genWalRecord(wal LogFileInfo) string {
	fileSize := wal.FileSize / 1048576
	return wal.FileName + "," + strconv.FormatUint(uint64(wal.StartTime), 10) + "," + strconv.FormatUint(uint64(wal.EndTime), 10) + "," + wal.TimeMode +
	"," + strconv.FormatUint(uint64(fileSize), 10) + "\n"
}

func RecordNewLogFile(ctx *BackupCtx, newLogFile LogFileInfo) error {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	ctx.status.CurLogFile = newLogFile
	//ctx.status.LogFiles = append(ctx.status.LogFiles, newLogFile)

	err := recordIncrementalBackupToMultiRemoteDB(ctx, newLogFile)
	if err != nil {
		ctx.logger.Printf("[ERROR] increpipeline plugin record backup info to remote db failed: %s", err.Error())
		return err
	}

	err = updateIncrementalStatusToMultiRemoteDB(ctx, false)
	if err != nil {
		ctx.logger.Printf("[ERROR] increpipeline plugin update backup info to remote db failed: %s", err.Error())
		return err
	}

	if !ctx.cmdline {
		var newLogFiles []LogFileInfo
		newLogFiles = append(newLogFiles, newLogFile)
		err = updateWalsMeta(ctx, newLogFiles)
		if err != nil {
			ctx.logger.Printf("[ERROR] increpipeline plugin update wals meta to backend failed: %s", err.Error())
			return err
		}
	}

	NotifyManagerCallback(ctx, "Running")

	return nil
}

func clearNewLogFile(ctx *BackupCtx, newLogFile LogFileInfo) {
	ctx.mutex.Lock()
	//ctx.status.LogFiles = make([]LogFileInfo, 0)
	ctx.mutex.Unlock()
}

func _backupWalLSN(ctx *BackupCtx, task *Task) error {
	ctx.logger.Printf("[INFO] backup current wal %s\n", task.name)

	chunkBuf := make([]byte, FileBufSize)
	offset := int64(0)
	name := task.name
	walname := strings.TrimSuffix(name, ".ready")

	frontendFile, err := OpenFile(ctx.frontend, "/pg_wal/"+walname)
	if err != nil {
		ctx.logger.Printf("[ERROR] open frontend file failed. walname[%s], err[%s]\n", walname, err.Error())
		return err
	}
	defer Close(ctx.frontend, frontendFile)

	frontendReader, ok := frontendFile.(io.Reader)
	if !ok {
		return errors.New("invalid frontendFile")
	}

	backendFile, err := CreateFile(ctx.backend, walname)
	if err != nil {
		ctx.logger.Printf("[ERROR] open backend file failed. walname[%s], err[%s]\n", walname, err.Error())
		return err
	}
	var newLogFile LogFileInfo
	newLogFile.FileName = walname
	now := time.Now().Unix()
	newLogFile.StartTime = now
	startTime := now
	var fileSize int64
	defer func() {
		Close(ctx.backend, backendFile)
		endTime := time.Now().Unix()
		ctx.status.Stage = "Running"
		newLogFile.FileSize = fileSize
		newLogFile.EndTime = endTime
		period := endTime - startTime
		if period > 0 {
			newLogFile.BackupSpeed = fileSize / period
		}
		RecordNewLogFile(ctx, newLogFile)
	}()

	backendWriter, ok := backendFile.(io.Writer)
	if !ok {
		return errors.New("invalid backendFile")
	}

	err = Fallocate(ctx, ctx.backend, backendFile, int64(1024*1024*1024))
	if err != nil {
		ctx.logger.Printf("[ERROR] fallocate backend file failed. walname[%s], err[%s]\n", walname, err.Error())
		return err
	}

	finish := false
	done := false
	for !done {
		if ctx.stop {
			done = true
		}

		currentwal, currentoffset, err := getCurrentWalFile(ctx)
		if err != nil {
			ctx.logger.Printf("[ERROR] get current wal failed. backup[%s], err[%s]\n", task.name, err.Error())
			return err
		}

		// ctx.logger.Printf("[INFO] current file name %s offset %d\n", currentwal, currentoffset)

		if task.name != currentwal {
			// we believe that backing up wal is finished writing, so we read this wal to the end immediately.
			_, _, err = CopyBuffer(io.Writer(backendWriter), io.Reader(frontendReader), chunkBuf, ctx.bpsController)
			if err != nil {
				ctx.logger.Printf("[ERROR] pipeline normal mode, copy file failed: %s\n", err.Error())
				return err
			}
			finish = true
			// FIXME not use hardcode
			fileSize = 1024 * 1024 * 1024
			break
		} else {
			// read current wal according to pg_current_wal_flush_lsn()
			// n, err := io.CopyN(io.Writer(backendWriter), io.Reader(frontendReader), currentoffset - offset)
			n, _, err := CopyBufferN(io.Writer(backendWriter), io.Reader(frontendReader), chunkBuf, currentoffset-offset, ctx.bpsController)
			if err != nil {
				if err == io.EOF {
					ctx.logger.Printf("[INFO] end of file %s\n", task.name)
					finish = true
					fileSize = currentoffset
					break
				} else {
					ctx.logger.Printf("[ERROR] pipeline normal mode, copy file failed: %s\n", err.Error())
				}
				return err
			}

			if n > 0 {
				ctx.logger.Printf("[INFO] write %d bytes, offset diff %d, wait one second to read next offset: current wal[%s], current offset[%d]\n", n, currentoffset-offset, currentwal, currentoffset)
			}
			offset = currentoffset
			fileSize = currentoffset
			time.Sleep(200 * time.Millisecond)
		}
	}

	if finish {
		ctx.logger.Printf("[INFO] wait 5 seconds for db generating .ready file. wal: %s\n", task.name)
		time.Sleep(5 * time.Second)

		RecordRenameWal(ctx, newLogFile)
		/*
		   err = RenameWal(ctx, ctx.frontend, walname)
		   if err != nil {
		       ctx.logger.Printf("[ERROR] rename .ready to .done on frontend failed. wal[%s] err[%s]\n", walname, err.Error())
		       return err
		   }
		*/
	}

	return nil
}

func RecordRenameWal(ctx *BackupCtx, logfile LogFileInfo) error {
	if ctx.cmdline {
		ctx.wallogsArray = append(ctx.wallogsArray, logfile)
	} else {
		ctx.wallogs <- logfile
	}
	return nil
}

func PolarRenameWal(ctx *BackupCtx, walname string) error {
	rows, err := queryDB(ctx.bkdbinfo, fmt.Sprintf("SET polar_rename_wal_ready_file='%s'", walname))
	if err != nil {
		ctx.logger.Printf("[ERROR] rename current wal file name failed. wal[%s] err[%s]\n", walname, err.Error())
		return err
	}
	defer rows.Close()

	return nil
}

func RenameWal(ctx *BackupCtx, fs fileSystem, walname string) error {
	var err error

	if ctx.frontend.name == "pfs" {
		err = PolarRenameWal(ctx, walname)
		if err != nil {
			ctx.logger.Printf("[ERROR] use db interface to rename .ready to .done failed. wal[%s], err[%s].\n", walname, err.Error())
			return err
		}
	} else {
		src := "/pg_wal/archive_status/" + walname + ".ready"
		dst := "/pg_wal/archive_status/" + walname + ".done"
		err = Rename(ctx, ctx.frontend, src, dst)
		if err != nil {
			ctx.logger.Printf("[ERROR] use fs interface to rename .ready to .done failed. src[%s], dst[%s], err[%s].\n", src, dst, err.Error())
			return err
		}
	}

	return nil
}

func GetSize(ctx *BackupCtx, fs fileSystem, filename string) (interface{}, error) {
	param := make(map[string]interface{})
	param["name"] = filename
	sz, err := fs.funcs["size"](fs.handle, param)

	if err != nil {
		ctx.logger.Printf("[ERROR] fs[%s] size[%s] failed, err[%s]\n", fs.name, filename, err.Error())
		return nil, err
	}

	return sz, nil
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

func Fallocate(ctx *BackupCtx, fs fileSystem, fp interface{}, size int64) error {
	param := make(map[string]interface{})
	param["file"] = fp
	param["size"] = size

	_, err := fs.funcs["fallocate"](fs.handle, param)
	if err != nil {
		ctx.logger.Printf("[ERROR] fs[%s] fallocate size[%d] failed, err[%s]\n", fs.name, size, err.Error())
		return err
	}

	ctx.logger.Printf("[DEBUG] fs[%s] fallocate size[%d]\n", fs.name, size)

	return nil
}

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

func Rename(ctx *BackupCtx, fs fileSystem, src string, dst string) error {
	param := make(map[string]interface{})
	param["source"] = src
	param["dest"] = dst
	_, err := fs.funcs["rename"](fs.handle, param)
	if err != nil {
		ctx.logger.Printf("[ERROR] rename failed. src[%s], dst[%s], err[%s]\n", src, dst, err.Error())
		return err
	}

	return nil
}

func getBlockSyncLock(ctx *BackupCtx) error {
	return waitBlockBackupDone(ctx.bkdbinfo)
}

func _backupWholeWalSeg(ctx *BackupCtx, task *Task) error {
	chunkBuf := make([]byte, FileBufSize)

	name := task.name

	var walstartTime, bkendTime, bkstartTime int64
	var walsize, backendFile, ret interface{}
	var backendWriter io.Writer
	var newLogFile LogFileInfo

	ctx.logger.Printf("[INFO] begin to backup. wal[%s]\n", name)
	wholewalname := strings.TrimSuffix(name, ".ready")

	walname := filepath.Base(wholewalname)

	frontendFile, err := OpenFile(ctx.frontend, "/pg_wal/"+walname)
	if err != nil {
		ctx.logger.Printf("[ERROR] open frontend file failed. walname[%s], err[%s]\n", walname, err.Error())
		return err
	}
	//defer Close(ctx, ctx.frontend, frontendFile)

	frontendReader, ok := frontendFile.(io.Reader)
	if !ok {
		err = errors.New("invalid frontendFile")
		goto clean
	}

	/*
	 * if wal exsit in backend, unlink it since it may be terminated when backup and it may be not complete.
	 * As the .ready file is exist, we can backup it again.
	 */
	ret, err = ExistFile(ctx.backend, walname)
	if err != nil {
		ctx.logger.Printf("[ERROR] exist file %s failed: %s\n", walname, err.Error())
		return err
	}
	if ret.(bool) {
		_, err := UnlinkFile(ctx.backend, walname)
		if err != nil {
			ctx.logger.Printf("[ERROR] unlink file %s failed: %s\n", walname, err.Error())
			return err
		}
	}

	backendFile, err = CreateFile(ctx.backend, walname)
	if err != nil {
		ctx.logger.Printf("[ERROR] open backend file failed. walname[%s], err[%s]\n", walname, err.Error())
		goto clean
	}
	//defer Close(ctx, ctx.backend, backendFile)

	newLogFile.FileName = walname
	walstartTime = time.Now().Unix()
	bkstartTime = time.Now().UnixNano()

	backendWriter, ok = backendFile.(io.Writer)
	if !ok {
		err = errors.New("invalid backendFile")
		goto clean
	}

	walsize, err = GetSize(ctx, ctx.frontend, "/pg_wal/"+walname)
	if err != nil {
		ctx.logger.Printf("[ERROR] get size failed. walname[%s], err[%s]\n", walname, err.Error())
		goto clean
	}
	newLogFile.FileSize = walsize.(int64)

	newLogFile.StartTime = walstartTime
	newLogFile.TimeMode = USINGREALTIME
	if path.Ext(walname) != ".history" {
		if ctx.conf.UseWalTime {
			var path interface{}
			path, err = ctx.frontend.funcs["getpath"](ctx.frontend.handle, nil)
			if err != nil {
				goto clean
			}
			err = getWalSegSize(ctx, path.(string)+"/pg_wal/"+walname, ctx.usePFS)
			if err != nil {
				goto clean
			}

			if ctx.enableBlockBackup {
				err = getBlockSyncLock(ctx)
				if err != nil {
					ctx.logger.Printf("[ERROR] get block sync lock failed: %s\n", err.Error())
					goto clean
				}
			}

			var info C.WallInfo
			ret := int(C.GetWalInfo(C.CString(path.(string)+"/pg_wal/"+walname), C.int(ctx.walSegSize), C.int(ctx.usePFS), &info, C.CString(ctx.blockBackupDir), C.bool(ctx.enableBlockBackup)))
			if ret == 0 {
				newLogFile.TimeMode = USINGWALTIME
				newLogFile.StartTime = int64(info.starttime)
				newLogFile.EndTime = int64(info.endtime)
				newLogFile.StartOffset = int64(info.startlsn)
				newLogFile.EndOffset = int64(info.endlsn)
				newLogFile.PrevOffset = int64(info.prevlsn)
			}
		}
	}

	if newLogFile.EndTime == 0 {
		newLogFile.EndTime = newLogFile.StartTime
	}

	err = Fallocate(ctx, ctx.backend, backendFile, walsize.(int64))
	if err != nil {
		ctx.logger.Printf("[ERROR] fallocate backend file failed. walname[%s], err[%s]\n", walname, err.Error())
		goto clean
	}

	if ctx.compress {
		if ctx.enableEncryption {
			chachaWriter := NewChaCha20Writer(ctx.encryptionPassword, backendWriter)
			lz4Writer := lz4.NewWriter(chachaWriter)
			_, _, err = CopyBuffer(io.Writer(lz4Writer), io.Reader(frontendReader), chunkBuf, ctx.bpsController)
			lz4Writer.Close()
		} else {
			lz4Writer := lz4.NewWriter(backendWriter)
			_, _, err = CopyBuffer(io.Writer(lz4Writer), io.Reader(frontendReader), chunkBuf, ctx.bpsController)
			lz4Writer.Close()
		}
	} else {
		if ctx.enableEncryption {
			chachaWriter := NewChaCha20Writer(ctx.encryptionPassword, backendWriter)
			_, _, err = CopyBuffer(io.Writer(chachaWriter), io.Reader(frontendReader), chunkBuf, ctx.bpsController)
		} else {
			_, _, err = CopyBuffer(io.Writer(backendWriter), io.Reader(frontendReader), chunkBuf, ctx.bpsController)
		}
	}

	if err != nil {
		ctx.logger.Printf("[ERROR] normal mode, copy file failed: %s\n", err.Error())
		goto clean
	}

	/*
	   err = RenameWal(ctx, ctx.frontend, walname)
	   if err != nil {
	       ctx.logger.Printf("[ERROR] rename .ready to .done on frontend failed. wal[%s] err[%s]\n", walname, err.Error())
	       goto clean
	   }
	*/

	bkendTime = time.Now().UnixNano()
	ctx.status.Stage = "Running"
	if bkendTime-bkstartTime > 1e3 {
		newLogFile.BackupSpeed = newLogFile.FileSize * 1e6 / ((bkendTime - bkstartTime) / 1e3)
	} else {
		newLogFile.BackupSpeed = newLogFile.FileSize * 1e9 / (bkendTime - bkstartTime)
	}

	Close(ctx.frontend, frontendFile)
	Close(ctx.backend, backendFile)

	err = RecordNewLogFile(ctx, newLogFile)
	/*
	 * If record to db or backupset failed, do not rename the .ready file, return error and then it will try backup again.
	 * Rename .ready is the last setp. Therefore, we must assure that the wal is backuped and the record is stored in db
	 * and backupset successfully before rename .ready.
	 */
	if err == nil {
		RecordRenameWal(ctx, newLogFile)
	}

	return err

clean:
	if frontendFile != nil {
		Close(ctx.frontend, frontendFile)
	}
	Close(ctx.backend, backendFile)
	return err
}

const (
	WALLSN = iota
	WholeWALSeg
)

type IncrBackupWalSeg struct {
	filename string
	mode     int
}

var record sync.Map

func eraseSyncMap(m *sync.Map) {
	m.Range(func(key interface{}, value interface{}) bool {
		m.Delete(key)
		return true
	})
}

func backupAction(backupCtx *BackupCtx) error {
	err := updateIncrementalStatusToMultiRemoteDB(backupCtx, true)
	if err != nil {
		return err
	}

	go func() {
		for !backupCtx.stop {
			select {
			case <-backupCtx.stopEvent:
				backupCtx.stop = true
				backupCtx.logger.Printf("[INFO] increpipeline backupAction get stop from ctx\n")
				break
			case <-time.After(3 * time.Second):
				break
			}
		}
	}()

	if !backupCtx.cmdline {
		go func() {
			for {
				select {
				case wal := <-backupCtx.wallogs:
					waitFullBackupDone(backupCtx.bkdbinfo)
					if backupCtx.job.PGType == "PolarDBFlex" {
						// In Flex, we should rename wal.ready in multi nodes. Therefore, we send the request to cm manager.
						err := NotifyManagerRenameWalCallback(backupCtx, wal)
						if err != nil {
							backupCtx.logger.Printf("[ERROR] rename multi wals failed: %s\n", err.Error())
						}
					} else {
						RenameWal(backupCtx, backupCtx.frontend, wal.FileName)
					}
				}
			}
		}()
	}

	var wait sync.WaitGroup

	// search .ready in pg_wal/archive_status in period
	// if found and not in map, add to channel
	ch := make(chan IncrBackupWalSeg, 1024)
	wait.Add(1)
	go func() {
		stop := false
		for !stop {
			if backupCtx.stop {
				backupCtx.logger.Printf("[INFO] increpipeline %s stop, [report] goroutine exit\n", backupCtx.action)
				eraseSyncMap(&record)
				stop = true
				break
			}

			if backupCtx.cmdline {
				stop = true
				backupCtx.stop = true
			}

			dir := "/pg_wal/archive_status/"
			files, err := ReadDir(backupCtx, backupCtx.frontend, dir)
			if err != nil {
				backupCtx.logger.Printf("[ERROR] read dir failed. path[%s], error[%s]\n", dir, err.Error())
				time.Sleep(1 * time.Second)
				continue
			}

			for _, file := range files {
				fileSuffix := path.Ext(file)
				// if fileSuffix == ".ready" && path.Ext(strings.TrimSuffix(file, ".ready")) == ".history" {
				//     continue
				// }
				_, exist := record.Load(strings.TrimSuffix(file, ".ready"))
				if fileSuffix == ".ready" && !exist {
					backupCtx.logger.Printf("[INFO] found wal file[%s] to backup\n", file)
					record.Store(strings.TrimSuffix(file, ".ready"), struct{}{})
					ch <- IncrBackupWalSeg{filename: strings.TrimSuffix(file, ".ready"), mode: WholeWALSeg}
				} else {
				}
			}

			if backupCtx.conf.RealTime {
				currentwal, _, err := getCurrentWalFile(backupCtx)
				if err != nil {
					backupCtx.logger.Printf("[ERROR] cannot get current wal file. err: %s\n", err.Error())
					time.Sleep(1 * time.Second)
					continue
				}

				if _, ok := record.Load(currentwal); !ok {
					record.Store(currentwal, struct{}{})
					backupCtx.logger.Printf("[DEBUG] get current wal. %s\n", currentwal)
					ch <- IncrBackupWalSeg{filename: currentwal, mode: WALLSN}
				}
			}

			time.Sleep(5 * time.Second)
		}
		close(ch)
		wait.Done()
	}()

	// get .ready from channel, add to queue
	wait.Add(1)
	go func() {
		stop := false
		for !stop {
			if backupCtx.stop {
				backupCtx.logger.Printf("[INFO] increpipeline %s stop, [report] goroutine exit\n", backupCtx.action)
				stop = true
			}
			for f := range ch {
				backupCtx.queue.AddTask(NewTaskWithParam(f.filename, f.mode))
			}
		}
		wait.Done()
	}()

	// backup
	time.Sleep(1 * time.Second)
	backupCtx.logger.Printf("[INFO] increpipeline start %d workers to do backup\n", backupCtx.workerCount)

	for i := 0; i < backupCtx.workerCount; i += 1 {
		wait.Add(1)
		go func() {
			_, err := _backupWorkflowWithoutArchive(backupCtx)
			if err != nil {
				backupCtx.logger.Printf("[ERROR] increpipeline backup failed, err[%s]\n", err.Error())
				atomic.AddInt32(&backupCtx.fail, 1)
			}
			wait.Done()
		}()
	}

	wait.Wait()

	if backupCtx.fail > 0 {
		backupCtx.logger.Printf("[INFO] increpipeline backup failed")
		return errors.New("increpipeline backup failed")
	}

	if backupCtx.cmdline {
		if len(backupCtx.wallogsArray) > 0 {
			err = updateWalsMeta(backupCtx, backupCtx.wallogsArray)
			if err != nil {
				return err
			}
		}
		for _, wal := range backupCtx.wallogsArray {
			waitFullBackupDone(backupCtx.bkdbinfo)
			err = RenameWal(backupCtx, backupCtx.frontend, wal.FileName)
			if err != nil {
				return err
			}
		}
	}

	if backupCtx.stop {
		backupCtx.status.Stage = "Stop"
		err := updateIncrementalStatusToMultiRemoteDB(backupCtx, true)
		if err != nil {
			return err
		}
		/*
		 * callback Stop again here in daemon mode, since several workflow may callback Running after receive stop command
		 */
		if !backupCtx.cmdline {
			NotifyManagerCallback(backupCtx, backupCtx.status.Stage)
		}
	}

	backupCtx.logger.Printf("[INFO] increpipeline backup done")

	return nil
}

func checkNeedRecovery(file string, logfiles []string) (bool, error) {
	for _, logfile := range logfiles {
		if logfile == file {
			return true, nil
		}
	}
	return false, nil
}

func restoreAction(backupCtx *BackupCtx) error {
	params := make(map[string]interface{})
	var files []string
	var err error

	params["path"] = ""
	_files, err := backupCtx.backend.funcs["readdir"](backupCtx.backend.handle, params)
	if err != nil {
		return err
	}
	files = _files.([]string)
	backupCtx.status.AllFilesCount = len(files)
	backupCtx.status.Stage = "Running"

	params["path"] = "/pg_wal"
	_oldfiles, err := backupCtx.frontend.funcs["readdir"](backupCtx.frontend.handle, params)
	if err != nil {
		backupCtx.status.Error = err.Error()
		backupCtx.status.Stage = "Failed"
		NotifyManagerCallback(backupCtx, backupCtx.status.Stage)
		return err
	}
	oldfiles, ok := _oldfiles.([]string)
	if !ok {
		return errors.New("readdir must be return []string")
	}
	var oldwals sync.Map
	for _, file := range oldfiles {
		oldwals.Store(file, struct{}{})
	}

	var needRecoveryWals []string
	if backupCtx.cmdline {
		needRecoveryWals = backupCtx.conf.RecoveryWals
	} else {
		needRecoveryWals = backupCtx.job.LogFiles
	}

	backupCtx.logger.Printf("[INFO] increpipeline need recovery %d wals, total %d wals\n", len(needRecoveryWals), len(files))

	recoveryCount := 0
	for _, file := range files {
		need, err := checkNeedRecovery(file, needRecoveryWals)
		if err != nil {
			backupCtx.logger.Printf("[INFO] increpipeline check recovery failed: %s\n", err.Error())
			return err
		}
		if !need {
			continue
		}

		backupCtx.logger.Printf("[INFO] increpipeline start recovery %s\n", file)

		oldwal := "pg_wal/" + file
		_, exist := oldwals.Load(oldwal)
		if exist {
			params["name"] = oldwal
			_, err = backupCtx.frontend.funcs["unlink"](backupCtx.frontend.handle, params)
			if err != nil {
				backupCtx.logger.Printf("[ERROR] increpipeline frontend unlink %s failed: %s\n", oldwal, err.Error())
				return err
			}
			backupCtx.logger.Printf("[INFO] increpipeline found %s in oldwals, delete it\n", oldwal)
		}

		buf := make([]byte, MinBufSize)
		params["name"] = file
		backendfile, err := backupCtx.backend.funcs["open"](backupCtx.backend.handle, params)
		if err != nil {
			backupCtx.logger.Printf("[ERROR] increpipeline open %s failed: %s\n", file, err.Error())
			return err
		}

		backendreader, ok := backendfile.(io.Reader)
		if !ok {
			return errors.New("invalid backendFile")
		}

		params["name"] = "pg_wal/" + file
		params["mode"] = uint(0600)
		frontendfile, err := backupCtx.frontend.funcs["create"](backupCtx.frontend.handle, params)
		if err != nil {
			backupCtx.logger.Printf("[ERROR] increpipeline frontend create %s failed: %s\n", file, err.Error())
			return err
		}
		frontendwriter := frontendfile.(io.Writer)

		var wsz int64
		if backupCtx.compress {
			if backupCtx.enableEncryption {
				chachaReader := NewChaCha20Reader(backupCtx.encryptionPassword, backendreader)
				lz4Reader := lz4.NewReader(chachaReader)
				wsz, _, err = CopyBuffer(frontendwriter, io.Reader(lz4Reader), buf, nil)
			} else {
				lz4Reader := lz4.NewReader(backendreader)
				wsz, _, err = CopyBuffer(frontendwriter, io.Reader(lz4Reader), buf, nil)
			}
		} else {
			if backupCtx.enableEncryption {
				chachaReader := NewChaCha20Reader(backupCtx.encryptionPassword, backendreader)
				wsz, _, err = CopyBuffer(frontendwriter, chachaReader, buf, nil)
			} else {
				wsz, _, err = CopyBuffer(frontendwriter, backendreader, buf, nil)
			}
		}

		params["file"] = frontendfile
		backupCtx.frontend.funcs["close"](backupCtx.frontend.handle, params)
		params["file"] = backendfile
		backupCtx.backend.funcs["close"](backupCtx.backend.handle, params)

		if err != nil {
			backupCtx.logger.Printf("[ERROR] increpipeline recovery file %s failed: %s\n", file, err.Error())
			return err
		}
		recoveryCount = recoveryCount + 1
		backupCtx.status.BackedSize = backupCtx.status.BackedSize + wsz
		backupCtx.logger.Printf("[INFO] increpipeline recovery %s done\n", file)
		atomic.AddInt32(&backupCtx.status.Download, 1)
	}

	if recoveryCount < len(needRecoveryWals) {
		var missWals string
		for _, ncWal := range needRecoveryWals {
			isFound := false
			for _, file := range files {
				if file == ncWal {
					isFound = true
					break
				}
			}
			if !isFound {
				missWals = missWals + ncWal + " "
			}
		}
		err = errors.New("Some wals needed to recovery miss. Please check the backup set. Miss wals's list: " + missWals)
		backupCtx.logger.Printf("[ERROR] increpipeline recovery failed: %s\n", err.Error())
		return err
	}

	backupCtx.status.Stage = "Finished"
	NotifyManagerCallback(backupCtx, backupCtx.status.Stage)
	return nil
}

func getWalSegSize(ctx *BackupCtx, walname string, usePfs int) error {
	if ctx.walSegSize == 0 {
		ctx.walSegSize = int(C.InitWalSize(C.CString(walname), C.int(ctx.usePFS)))
		if ctx.walSegSize == 0 {
			return errors.New("invalid wallog file")
		}
	}
	return nil
}

func recordIncrementalBackupToMultiRemoteDB(ctx *BackupCtx, logfile LogFileInfo) error {
	var err error

	tryMax := 3
	for i := 0; i < tryMax; i++ {
		err := recordIncrementalBackupToRemoteDB(ctx, ctx.bkdbinfo, logfile)
		if err == nil {
			break
		}
		ctx.logger.Printf("[WARNING] pgpipeline try recordIncrementalBackup %s ToRemoteDB %d times failed: %s\n", logfile.FileName, i+1, err.Error())
		if i+1 < tryMax {
			time.Sleep(60 * time.Second)
		}
	}
	if err != nil {
		return err
	}

	if ctx.mtdbinfo != nil {
		err := recordIncrementalBackupToRemoteDB(ctx, ctx.mtdbinfo, logfile)
		if err != nil {
			return err
		}
	}

	return nil
}

func recordIncrementalBackupToRemoteDB(ctx *BackupCtx, dbinfo *DBInfo, logfile LogFileInfo) error {
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
	if err != nil {
		ctx.logger.Printf("[ERROR] increpipeline exe sql: %s failed: %s\n", sqlStatement, err.Error())
		return err
	}
	defer rows.Close()

	var sql string
	if dbinfo.dbType == "pgsql" {
		sql = `
        INSERT INTO backup_meta (instanceid, backupid, backupjobid, file, backuptype, starttime, endtime, location, status)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        on conflict (instanceid, backupid, file) do update set starttime=excluded.starttime,endtime=excluded.endtime`
	} else if dbinfo.dbType == "mysql" {
		sql = `
        INSERT INTO backup_meta (instanceid, backupid, backupjobid, file, backuptype, starttime, endtime, location, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE starttime=VALUES(starttime), endtime=VALUES(endtime)`
	} else {
		return errors.New("Not support db type")
	}
	_, err = dbinfo.db.Exec(sql, ctx.status.InstanceID, ctx.status.BackupID, ctx.status.BackupJobID, logfile.FileName, "wal", logfile.StartTime, logfile.EndTime, ctx.status.Location, "Finished")
	if err != nil {
		ctx.logger.Printf("[ERROR] increpipeline exe sql: %s failed: %s\n", sql, err.Error())
		return err
	}

	ctx.logger.Printf("[INFO] increpipeline exe sql: %s with wal %s done\n", sql, logfile.FileName)

	return nil
}

func updateIncrementalStatusToMultiRemoteDB(ctx *BackupCtx, forceUpdate bool) error {
	tryMax := 1
	if forceUpdate {
		tryMax = 3
	}

	var err1, err2 error
	if ctx.bkdbinfo != nil {
		for i := 0; i < tryMax; i++ {
			err1 = updateIncrementalStatusToRemoteDB(ctx, ctx.bkdbinfo)
			if err1 != nil {
				ctx.logger.Printf("[ERROR] updateIncrementalStatusToRemoteDBfailed: %s, try %d times\n", err1.Error(), i+1)
				if i+1 < tryMax {
					time.Sleep(5 * time.Second)
				}
			} else {
				ctx.logger.Printf("[INFO] updateIncrementalStatusToRemoteDB done\n")
				break
			}
		}
	}

	if ctx.mtdbinfo != nil {
		for i := 0; i < tryMax; i++ {
			err2 = updateIncrementalStatusToRemoteDB(ctx, ctx.mtdbinfo)
			if err2 != nil {
				ctx.logger.Printf("[ERROR] updateIncrementalStatusToRemoteDBfailed: %s, try %d times\n", err2.Error(), i+1)
				if i+1 < tryMax {
					time.Sleep(5 * time.Second)
				}
			} else {
				ctx.logger.Printf("[INFO] updateIncrementalStatusToRemoteDB done\n")
				break
			}
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
		return errors.New(errstr)
	}

	return nil
}

func updateIncrementalStatusToRemoteDB(ctx *BackupCtx, dbinfo *DBInfo) error {
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
	if err != nil {
		ctx.logger.Printf("[ERROR] increpipeline exe sql: %s failed: %s\n", sqlStatement, err.Error())
		return err
	}
	defer rows.Close()

	if dbinfo.dbType == "pgsql" {
		sqlStatement = `
        INSERT INTO backup_meta (instanceid, backupid, backupjobid, file, backuptype, starttime, endtime, location, status)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        on conflict (instanceid, backupid, file) do update set backupjobid=excluded.backupjobid,endtime=excluded.endtime,status=excluded.status`
	} else if dbinfo.dbType == "mysql" {
		sqlStatement = `
        INSERT INTO backup_meta (instanceid, backupid, backupjobid, file, backuptype, starttime, endtime, location, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE backupjobid=VALUES(backupjobid), endtime=VALUES(endtime), status=VALUES(status)`
	} else {
		return errors.New("Not support db type")
	}

	_, err = dbinfo.db.Exec(sqlStatement, ctx.status.InstanceID, ctx.status.BackupID, ctx.status.BackupJobID, "default", "incremental", 0, time.Now().Unix(), ctx.status.Location, ctx.status.Stage)
	if err != nil {
		ctx.logger.Printf("[ERROR] increpipeline exe sql: %s failed: %s\n", sqlStatement, err.Error())
		return err
	}
	ctx.logger.Printf("[INFO] increpipeline exe sql: %s done iid:%s, bid:%s, file:%s, bjid:%s, st:%s\n", sqlStatement, ctx.status.InstanceID, ctx.status.BackupID, "default", ctx.status.BackupJobID, ctx.status.Stage)

	return nil
}

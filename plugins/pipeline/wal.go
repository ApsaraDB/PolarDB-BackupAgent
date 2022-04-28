package main

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

type WalInitConf struct {
	Action   string `json:"Action"`
	WaitTime int    `json:"WaitTime"`
	Role     string `json:"Role"`
}

type WalCtx struct {
	logger *log.Logger
	conf   *WalInitConf
}

func PluginInit(ctx interface{}) (interface{}, error) {
	walCtx := &WalCtx{}

	m, ok := ctx.(map[string]interface{})
	if !ok {
		return nil, errors.New("invalid init ctx")
	}

	_logger, ok := m["logger"]
	if !ok {
		return nil, errors.New("ctx.logger miss")
	}

	logger, ok := _logger.(*log.Logger)
	if !ok {
		return nil, errors.New("ctx.logger must be *log.Logger")
	}

	_conf, ok := m["conf"]
	if !ok {
		return nil, errors.New("ctx.conf miss")
	}
	conf, ok := _conf.([]byte)
	if !ok {
		return nil, errors.New("ctx.conf must be []byte")
	}

	var initConf WalInitConf
	err := json.Unmarshal(conf, &initConf)
	if err != nil {
		return nil, errors.New("parse plugin conf failed")
	}

	action := initConf.Action
	if action != "renamewal" {
		return nil, errors.New("not support action: " + action)
	}

	walCtx.conf = &initConf
	walCtx.logger = logger

	return walCtx, nil
}

func PluginRun(ctx interface{}, param interface{}) error {
	var err error
	walCtx, ok := ctx.(*WalCtx)
	if !ok {
		err = errors.New("ctx must be *WalCtx")
		return err
	}

	if walCtx == nil {
		err = errors.New("ctx must not be empty")
		return err
	}

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

	_req, ok := extern["req"]
	if ok {
		req, ok := _req.(map[string]interface{})
		if !ok {
			walCtx.logger.Printf("[ERROR] wal plugin Message.Data[extern][req] must be map[string]interface{}\n")
			return errors.New("param.extern.req must be map[string]interface{}")
		}
		content, err := json.MarshalIndent(&req, "", "\t")
		if err != nil {
			walCtx.logger.Printf("[ERROR] wal plugin Message.Data[extern][req] -> json failed\n")
			return errors.New("param.extern.task -> json failed")
		}

		var whreq WalHelperRequest
		err = json.Unmarshal(content, &whreq)
		if err != nil {
			walCtx.logger.Printf("[ERROR] wal plugin Message.Data[extern][req] must be RenameWalRequest\n")
			return errors.New("param.extern.req must be RecoveryRequest failed")
		}

		switch whreq.Action {
		case "renamewal":
			err = renamewal(walCtx, &whreq)
			break
		case "forcerenamewals":
			err = forceRenameWals(walCtx, &whreq)
			break
		case "clearblockdir":
			err = clearBlockDir(walCtx, &whreq)
			break
		case "lockBlockDir":
			err = lockBlockDir(walCtx, &whreq)
			break
		case "clearBlockIndex":
			err = clearBlockIndex(walCtx, &whreq)
			break
		case "unlockBlockDir":
			err = unlockBlockDir(walCtx, &whreq)
			break
		}

		if err != nil {
			walCtx.logger.Printf("[ERROR] wal plugin do %s failed: %s\n", whreq.Action, err.Error())
		} else {
			walCtx.logger.Printf("[INFO] wal plugin do %s successfully\n", whreq.Action)
		}
	}

	return err
}

func clearBlockDir(ctx *WalCtx, whreq *WalHelperRequest) error {
	bkfiles, err := ioutil.ReadDir(whreq.BlockBackupDir)
	if err != nil {
		return err
	}

	for _, f := range bkfiles {
		err = os.Remove(whreq.BlockBackupDir + "/" + f.Name())
		if err != nil {
			err = errors.New("remove file in block backup dir failed: " + err.Error())
			break
		}
	}
	return err
}

func lockBlockDir(ctx *WalCtx, whreq *WalHelperRequest) error {
	lockFileName := whreq.BlockBackupDir + "/" + "global.lock"
	_, err := os.Create(lockFileName)
	return err
}

func clearBlockIndex(ctx *WalCtx, whreq *WalHelperRequest) error {
	lockFileName := whreq.BlockBackupDir + "/" + "global.lock"

	// clear block backup dir
	bkfiles, err := ioutil.ReadDir(whreq.BlockBackupDir)
	for _, f := range bkfiles {
		fileName := whreq.BlockBackupDir + "/" + f.Name()

		if fileName == lockFileName {
			continue
		}

		err = os.Remove(fileName)
		if err != nil {
			err = errors.New("remove file in block backup dir failed: " + err.Error())
			break
		}
	}

	return err
}

func unlockBlockDir(ctx *WalCtx, whreq *WalHelperRequest) error {
	lockFileName := whreq.BlockBackupDir + "/" + "global.lock"

	err := os.Remove(lockFileName)
	if err != nil {
		err = errors.New("remove lock file in block backup dir failed: " + err.Error())
	}
	return err
}

func renamewal(walCtx *WalCtx, whreq *WalHelperRequest) error {
	var err error

	var rel string
	if walCtx.conf.Role == "logger" {
		rel = "/polar_datamax"
	}

	src := whreq.BackupFolder + rel + "/pg_wal/archive_status/" + whreq.CurLogFile.FileName + ".ready"
	dst := whreq.BackupFolder + rel + "/pg_wal/archive_status/" + whreq.CurLogFile.FileName + ".done"

	isExist := false
	times := walCtx.conf.WaitTime
	for i := 0; i < times; i++ {
		_, err = os.Stat(src)
		if os.IsNotExist(err) {
			time.Sleep(1 * time.Second)
		} else {
			isExist = true
			break
		}
	}

	if isExist {
		err = os.Rename(src, dst)
		if err != nil {
			walCtx.logger.Printf("[INFO] wal plugin rename %s failed: %s\n", whreq.CurLogFile.FileName, err.Error())
		} else {
			walCtx.logger.Printf("[INFO] wal plugin rename %s done\n", whreq.CurLogFile.FileName)
		}
	} else {
		err = errors.New("search " + src + " more than " + strconv.Itoa(times) + " seconds")
		walCtx.logger.Printf("[INFO] wal plugin rename %s failed: %s\n", whreq.CurLogFile.FileName, err.Error())
	}

	return err
}

func forceRenameWals(walCtx *WalCtx, whreq *WalHelperRequest) error {
	var err error

	status_dir := whreq.BackupFolder + "/pg_wal/archive_status"

	file, err := os.Open(status_dir)
	if err != nil {
		walCtx.logger.Printf("[ERROR] open archive_status dir %s failed: %s\n", status_dir, err.Error())
		return err
	}

	success := 0
	fail := 0
	status_files, err := file.Readdirnames(-1)
	if err != nil {
		walCtx.logger.Printf("[ERROR] read archive_status dir %s failed: %s\n", status_dir, err.Error())
		return err
	}
	for _, status_file := range status_files {
		if path.Ext(status_file) == ".ready" {
			status_file_done := strings.TrimSuffix(status_file, ".ready") + ".done"
			err = os.Rename(status_dir + "/" + status_file, status_dir + "/" + status_file_done)
			if err != nil {
				walCtx.logger.Printf("[ERROR] wal plugin force rename %s/%s -> %s/%s failed: %s\n", status_dir, status_file, status_dir, status_file_done, err.Error())
				fail = fail + 1
			} else {
				walCtx.logger.Printf("[INFO] wal plugin force rename %s/%s -> %s/%s done\n", status_dir, status_file, status_dir, status_file_done)
				success = success + 1
			}
		}
	}

	walCtx.logger.Printf("[INFO] force rename wals of dir: %s done, success: %d, fail: %d\n", status_dir, success, fail)

	return err
}

func PluginExit(ctx interface{}) error {
	return nil
}

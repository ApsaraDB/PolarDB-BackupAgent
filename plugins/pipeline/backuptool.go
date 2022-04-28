package main

/*
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "pfs_api.h"
*/
import "C"
import (
	"encoding/json"
	"errors"
	"path"
	"syscall"
	"unsafe"
)

type LabelInitConf struct {
	Cluster  string `json:"Cluster"`
	Pbd      string `json:"Pbd"`
	Action   string `json:"Action"`
	FileName string `json:"FileName"`
	Content  string `json:"Content"`
}

type labelCtx struct {
	conf   LabelInitConf
	notify chan []byte
}

func PluginInit(ctx interface{}) (interface{}, error) {
	m, ok := ctx.(map[string]interface{})
	if !ok {
		return nil, errors.New("invalid init ctx")
	}

	_conf, ok := m["conf"]
	if !ok {
		return nil, errors.New("ctx.conf miss")
	}
	conf, ok := _conf.([]byte)
	if !ok {
		return nil, errors.New("ctx.conf must be []byte")
	}

	_notify, ok := m["notify"]
	if !ok {
		return nil, errors.New("ctx.notify miss")
	}
	notify, ok := _notify.(chan []byte)
	if !ok {
		return nil, errors.New("ctx.notify must be chan []byte")
	}

	var initConf LabelInitConf
	err := json.Unmarshal(conf, &initConf)
	if err != nil {
		return nil, errors.New("parse plugin conf failed")
	}

	lCtx := &labelCtx{
		conf:   initConf,
		notify: notify,
	}
	return lCtx, nil
}

func PluginExit(ctx interface{}) error {
	return nil
}

func isValidAction(action string) bool {
	return action == "backup" || action == "restore" || action == "pfswrite" || action == "pfsread"
}

func initTask(ctx *labelCtx, param map[string]interface{}) error {
	_extern, ok := param["extern"]
	if !ok {
		return nil
	}

	extern, ok := _extern.(map[string]interface{})
	if !ok {
		return errors.New("param.extern must be map[string]interface{}")
	}
	_conf, ok := extern["backuptool"]
	if !ok {
		return nil
	}
	conf, ok := _conf.(map[string]interface{})
	if !ok {
		return errors.New("param.extern.restoretool must be map[string]interface{}")
	}
	for key, _v := range conf {
		value, ok := _v.(string)
		if !ok {
			return errors.New("invalid extern conf format")
		}
		switch key {
		case "Action":
			ctx.conf.Action = value
			break
		case "Content":
			ctx.conf.Content = value
			break
		case "FileName":
			ctx.conf.FileName = value
			break
		default:
			break
		}
	}
	return nil
}

func PluginRun(ctx interface{}, param interface{}) error {
	lCtx, ok := ctx.(*labelCtx)
	if !ok {
		return errors.New("ctx must be *labelCtx")
	}

	m, ok := param.(map[string]interface{})
	if !ok {
		return errors.New("param must be map[string]interface{}")
	}
	err := initTask(lCtx, m)
	if err != nil {
		return err
	}

	action := lCtx.conf.Action
	if !isValidAction(action) {
		return errors.New("invalid action")
	}

	flags := 0x1000 | 0x0002 | 0x0010
	ret, err := C.pfs_mount(C.CString(lCtx.conf.Cluster), C.CString(lCtx.conf.Pbd), 0, C.int(flags))
	if int(ret) != 0 {
		return err
	}

	defer func() {
		C.pfs_umount((C.CString(lCtx.conf.Pbd)))
	}()

	switch action {
	case "backup":
		err = backupLabel(lCtx)
		if err == nil {
			err = syscall.EAGAIN
		}
		break
	case "restore":
		err = restoreLabel(lCtx)
		break
	default:
		return nil
	}
	return err
}

func uploadContent(ctx *labelCtx, content, filename string) error {
	externConf := make(map[string]interface{})

	restoretoolconf := make(map[string]interface{})
	restoretoolconf["UploadName"] = "/data/backup_label"
	restoretoolconf["Content"] = content
	restoretoolconf["Action"] = "backup"

	externConf["restoretool"] = restoretoolconf

	msg := &Message{
		Cmd:    "backup",
		Plugin: "restoretool",
		Data:   externConf,
	}
	err := SendMsg(msg, ctx.notify)
	return err
}

func readfile(filename, pbd string) ([]byte, error) {
	flags := 00
	filename = path.Join("/", pbd, filename)
	fd, err := C.pfs_open(C.CString(filename), C.int(flags), C.uint(0400))
	if fd < 0 {
		return nil, err
	}

	buf := make([]byte, 8192)
	c_buf := unsafe.Pointer(&buf[0])
	c_buf_len := C.ulong(len(buf))
	_, err = C.pfs_read(C.int(fd), c_buf, c_buf_len)
	if err != nil {
		return nil, err
	}
	C.pfs_close(C.int(fd))
	return buf, nil
}

func writeContent(ctx *labelCtx) error {
	filename := path.Join("/", ctx.conf.Pbd, ctx.conf.FileName)
	buf := []byte(ctx.conf.Content)
	flags := 01
	fd, err := C.pfs_open(C.CString(filename), C.int(flags), C.uint(0200))
	if fd < 0 {
		return err
	}
	c_buf := unsafe.Pointer(&buf[0])
	c_buf_len := C.ulong(len(buf))
	_, err = C.pfs_write(C.int(fd), c_buf, c_buf_len)
	if err != nil {
		return err
	}
	C.pfs_close(C.int(fd))
	return nil
}

func getBackupLabel(content []byte) (string, error) {
	if len(content) < 284 {
		return "", errors.New("invalid pgcontroldata")
	}
	ctrl, err := ParsePgControl(content)
	if err != nil {
		return "", err
	}
	str := SetBackupLabelInfo(ctrl, "standby", "backup")
	return str, nil
}

func backupLabel(lCtx *labelCtx) error {
	buf, err := readfile(lCtx.conf.FileName, lCtx.conf.Pbd)
	if err != nil {
		return err
	}
	content, err := getBackupLabel(buf)
	if err != nil {
		return err
	}
	return uploadContent(lCtx, content, "backup_label")
}

func restoreLabel(lCtx *labelCtx) error {
	return writeContent(lCtx)
}

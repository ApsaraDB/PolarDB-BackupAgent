package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"gitlab.alibaba-inc.com/aliyun-dbs/dedup-gateway-sdk/common"
	"gitlab.alibaba-inc.com/aliyun-dbs/dedup-gateway-sdk/protobuf"
	"gitlab.alibaba-inc.com/aliyun-dbs/dedup-gateway-sdk/sdk"
	"io"
	"io/ioutil"
	"os"
	"syscall"
)

type JobInitConf struct {
	BackupID   string `json:"BackupID"`
	InstanceID string `json:"InstanceID"`
	Gateway    string `json:"Gateway"`
	FileName   string `json:"FileName"`
	UploadName string `json:"UploadName"`
	Action     string `json:"Action"`
	Content    string `json:"Content"`
}

type UploadCtx struct {
	conf   JobInitConf
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

	var initConf JobInitConf
	err := json.Unmarshal(conf, &initConf)
	if err != nil {
		return nil, errors.New("parse plugin conf failed")
	}

	uploadCtx := &UploadCtx{
		conf:   initConf,
		notify: notify,
	}
	return uploadCtx, nil
}

func initTask(uCtx *UploadCtx, param map[string]interface{}) error {
	_extern, ok := param["extern"]
	if !ok {
		return nil
	}

	extern, ok := _extern.(map[string]interface{})
	if !ok {
		return errors.New("param.extern must be map[string]interface{}")
	}
	_conf, ok := extern["restoretool"]
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
			uCtx.conf.Action = value
			break
		case "Content":
			uCtx.conf.Content = value
			break
		case "UploadName":
			uCtx.conf.UploadName = value
			break
		default:
			break
		}
	}
	return nil
}

func PluginRun(ctx interface{}, param interface{}) error {
	uCtx, ok := ctx.(*UploadCtx)
	if !ok {
		return errors.New("ctx must be *UploadCtx")
	}

	m, ok := param.(map[string]interface{})
	if !ok {
		return errors.New("param must be map[string]interface{}")
	} else {
		err := initTask(uCtx, m)
		if err != nil {
			return err
		}
	}

	action := uCtx.conf.Action
	switch action {
	case "backup":
		return UploadContent(uCtx)
	case "restore":
		return DownloadContent(uCtx)
	case "upload":
		return UploadFile(uCtx)
	case "download":
		return DownloadFile(uCtx)
	default:
		return errors.New("invalid action")
	}
}

func DownloadContent(uCtx *UploadCtx) error {
	client, err := sdk.NewObjectStorageClient(uCtx.conf.Gateway)
	if err != nil {
		return err
	}

	key := common.GetFileKey(uCtx.conf.BackupID, uCtx.conf.UploadName)

	reader, err := client.OpenObjectForRead(&protobuf.OpenObjectForReadRequest{
		Key:             key,
		ReadStartOffset: 0,
		Namespace:       uCtx.conf.InstanceID,
	})
	if err != nil {
		return err
	}

	content, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	return WritePFS(uCtx, uCtx.conf.UploadName, string(content))
}

func WritePFS(uCtx *UploadCtx, filename, content string) error {
	externConf := make(map[string]interface{})
	backuptoolconf := make(map[string]interface{})

	backuptoolconf["FileName"] = filename
	backuptoolconf["Content"] = content
	backuptoolconf["Action"] = "restore"

	externConf["backuptool"] = backuptoolconf

	msg := &Message{
		Cmd:    "restore",
		Plugin: "backuptool",
		Data:   externConf,
	}
	err := SendMsg(msg, uCtx.notify)
	if err == nil {
		err = syscall.EAGAIN
	}
	return err
}

func DownloadFile(uCtx *UploadCtx) error {
	client, err := sdk.NewObjectStorageClient(uCtx.conf.Gateway)
	if err != nil {
		return err
	}

	key := common.GetFileKey(uCtx.conf.BackupID, uCtx.conf.FileName)

	reader, err := client.OpenObjectForRead(&protobuf.OpenObjectForReadRequest{
		Key:             key,
		ReadStartOffset: 0,
		Namespace:       uCtx.conf.InstanceID,
	})
	if err != nil {
		return err
	}

	file, err := os.Create(uCtx.conf.FileName)
	if err != nil {
		return err
	}
	_, err = io.Copy(file, reader)
	if err != nil {
		return err
	}
	file.Close()
	return nil
}

func UploadContent(uCtx *UploadCtx) error {
	client, err := sdk.NewObjectStorageClient(uCtx.conf.Gateway)
	if err != nil {
		return err
	}
	name := uCtx.conf.UploadName
	key := common.GetFileKey(uCtx.conf.BackupID, name)

	createStreamReq := &protobuf.OpenObjectForWriteRequest{
		Namespace:         uCtx.conf.InstanceID,
		Key:               key,
		Meta:              []byte(name),
		CreatedIfNotExist: true,
	}
	resp, err := client.OpenObjectForWrite(createStreamReq)
	if err != nil {
		return err
	}

	if resp.LastUploadOffset != 0 {
		return os.ErrExist
	}

	msg := []byte(uCtx.conf.Content)
	r := bytes.NewReader(msg)
	err = client.AppendObject(uCtx.conf.InstanceID, key, 0, r)
	if err != nil {
		return nil
	}
	_, err = client.CloseObjectForWrite(&protobuf.CloseObjectForWriteRequest{
		Namespace: uCtx.conf.InstanceID,
		Key:       key,
	})
	if err != nil {
		return nil
	}
	client.Close()
	return nil
}

func UploadFile(uCtx *UploadCtx) error {
	client, err := sdk.NewObjectStorageClient(uCtx.conf.Gateway)
	if err != nil {
		return err
	}
	name := uCtx.conf.UploadName
	key := common.GetFileKey(uCtx.conf.BackupID, name)

	createStreamReq := &protobuf.OpenObjectForWriteRequest{
		Namespace:         uCtx.conf.InstanceID,
		Key:               key,
		Meta:              []byte(name),
		CreatedIfNotExist: true,
	}
	resp, err := client.OpenObjectForWrite(createStreamReq)
	if err != nil {
		return err
	}

	if resp.LastUploadOffset != 0 {
		return os.ErrExist
	}

	msg, err := ioutil.ReadFile(uCtx.conf.FileName)
	if err != nil {
		return err
	}

	r := bytes.NewReader(msg)
	err = client.AppendObject(uCtx.conf.InstanceID, key, 0, r)
	if err != nil {
		return nil
	}
	_, err = client.CloseObjectForWrite(&protobuf.CloseObjectForWriteRequest{
		Namespace: uCtx.conf.InstanceID,
		Key:       key,
	})
	if err != nil {
		return nil
	}
	client.Close()
	return err
}

func PluginExit(ctx interface{}) error {
	return nil
}

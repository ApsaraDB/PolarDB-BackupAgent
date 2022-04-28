package main

import (
	"encoding/json"
	"errors"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"io"
	"os"
	"path"
	"path/filepath"
)

type OSSConf struct {
	Endpoint   string `json:"Endpoint"`
	ID         string `json:"ID"`
	Key        string `json:"Key"`
	Bucket     string `json:"Bucket"`
	InstanceID string `json:"InstanceID"`
	BackupID   string `json:"BackupID"`
	Secure     bool   `json:"Secure"`
}

const (
	OSS_MODE_READ  = 1
	OSS_MODE_WRITE = 2
)

type OSSCtx struct {
	bucket   string
	client   *oss.Client
	instance string
	backupid string
	path     string
}

type OSSFile struct {
	name   string
	mode   int
	r      *os.File
	w      *os.File
	stop   chan bool
	reader io.ReadCloser
}

func PluginInit(ctx interface{}) (interface{}, error) {
	return nil, nil
}

func PluginRun(ctx interface{}, param interface{}) error {
	return nil
}

func PluginExit(ctx interface{}) error {
	return nil
}

func (file *OSSFile) Read(buf []byte) (int, error) {
	if file.mode != OSS_MODE_READ {
		return 0, errors.New("only support read in download mode")
	}
	return file.reader.Read(buf)
}

func (file *OSSFile) Write(buf []byte) (int, error) {
	if file.mode != OSS_MODE_WRITE {
		return 0, errors.New("only support write in upload mode")
	}
	return file.w.Write(buf)
}

func OSSInit(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	if ctx != nil {
		return nil, errors.New("ctx must be nil")
	}

	_conf, ok := param["conf"]
	if !ok {
		return nil, errors.New("param.conf miss")
	}
	conf, ok := _conf.([]byte)
	if !ok {
		return nil, errors.New("param.conf must be []byte")
	}

	var initConf OSSConf
	err := json.Unmarshal(conf, &initConf)
	if err != nil {
		return nil, errors.New("parse plugin conf failed")
	}

	client, err := oss.New(initConf.Endpoint, initConf.ID, initConf.Key)
	if err != nil {
		return nil, err
	}

	_path := initConf.ID + ":" + initConf.Key + "@" + initConf.Endpoint + "/" + initConf.Bucket

	ossctx := &OSSCtx{
		client:   client,
		bucket:   initConf.Bucket,
		instance: initConf.InstanceID,
		backupid: initConf.BackupID,
		path:     _path,
	}
	return ossctx, nil
}

func OSSOpen(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	oss, ok := ctx.(*OSSCtx)
	if !ok {
		return nil, errors.New("ctx must be *OSSCtx")
	}
	_filepath, ok := param["name"]
	if !ok {
		return nil, errors.New("param.name miss")
	}

	filepath, ok := _filepath.(string)
	if !ok {
		return nil, errors.New("param.name msut be string")
	}

	name := path.Join(oss.instance, oss.backupid, filepath)

	bucket, err := oss.client.Bucket(oss.bucket)
	if err != nil {
		return nil, errors.New("oss get bucket failed:" + err.Error())
	}

	object, err := bucket.GetObject(name)
	if err != nil {
		return nil, errors.New("oss get object failed:" + err.Error())
	}

	file := &OSSFile{
		mode:   OSS_MODE_READ,
		name:   name,
		reader: object,
	}
	return file, nil
}

func OSSExist(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	oss, ok := ctx.(*OSSCtx)
	if !ok {
		return nil, errors.New("ctx must be *OSSCtx")
	}
	_filepath, ok := param["path"]
	if !ok {
		return nil, errors.New("param.path miss")
	}

	filepath, ok := _filepath.(string)
	if !ok {
		return nil, errors.New("param.path msut be string")
	}

	name := path.Join(oss.instance, oss.backupid, filepath)

	bucket, err := oss.client.Bucket(oss.bucket)
	if err != nil {
		return nil, errors.New("oss get bucket failed: " + err.Error())
	}

	isExist, err := bucket.IsObjectExist(name)
	if err != nil {
		return nil, errors.New("oss exist object failed: " + err.Error())
	}

	return isExist, nil
}

func OSSCreate(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	ossctx, ok := ctx.(*OSSCtx)
	if !ok {
		return nil, errors.New("ctx must be *OSSCtx")
	}
	_filepath, ok := param["name"]
	if !ok {
		return nil, errors.New("param.name miss")
	}

	filepath, ok := _filepath.(string)
	if !ok {
		return nil, errors.New("param.name msut be string")
	}

	r, w, err := os.Pipe()
	if err != nil {
		return nil, errors.New("create pipe failed")
	}

	name := path.Join(ossctx.instance, ossctx.backupid, filepath)
	file := &OSSFile{
		mode: OSS_MODE_WRITE,
		name: name,
		r:    r,
		w:    w,
		stop: make(chan bool, 1),
	}
	go func() {
		reader := file.r

		bucket, err := ossctx.client.Bucket(ossctx.bucket)
		if err != nil {
			file.r.Close()
			file.stop <- true
			return
		}

		storageType := oss.ObjectStorageClass(oss.StorageStandard)
		objectAcl := oss.ObjectACL(oss.ACLPublicRead)

		err = bucket.PutObject(file.name, reader, storageType, objectAcl)
		if err != nil {
			file.r.Close()
			file.stop <- true
			return
		}

		file.r.Close()
		file.stop <- true
	}()
	return file, nil
}

func OSSWrite(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*OSSCtx)
	if !ok {
		return nil, errors.New("ctx must be *OSSCtx")
	}
	_file, ok := param["file"]
	if !ok {
		return nil, errors.New("param.file miss")
	}
	file, ok := _file.(*OSSFile)
	if !ok {
		return nil, errors.New("param.file msut be *OSSFile")
	}

	_buf, ok := param["buf"]
	if !ok {
		return nil, errors.New("param.buf miss")
	}
	buf, ok := _buf.([]byte)
	if !ok {
		return nil, errors.New("param.buf msut be []byte")
	}
	return file.w.Write(buf)
}

func OSSRead(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*OSSCtx)
	if !ok {
		return nil, errors.New("ctx must be *OSSCtx")
	}
	_file, ok := param["file"]
	if !ok {
		return nil, errors.New("param.file miss")
	}
	file, ok := _file.(*OSSFile)
	if !ok {
		return nil, errors.New("param.file msut be *OSSFile")
	}
	_buf, ok := param["buf"]
	if !ok {
		return nil, errors.New("param.buf miss")
	}
	buf, ok := _buf.([]byte)
	if !ok {
		return nil, errors.New("param.buf msut be []byte")
	}

	return file.reader.Read(buf)
}

func OSSClose(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*OSSCtx)
	if !ok {
		return nil, errors.New("ctx must be *OSSCtx")
	}
	_file, ok := param["file"]
	if !ok {
		return nil, errors.New("param.file miss")
	}
	file, ok := _file.(*OSSFile)
	if !ok {
		return nil, errors.New("param.file msut be *OSSFile")
	}
	if file.mode == OSS_MODE_WRITE {
		err := file.w.Close()
		if err != nil {
			return nil, err
		}
		_ = <-file.stop
		return nil, nil
	} else if file.mode == OSS_MODE_READ {
		return nil, file.reader.Close()
	}
	return nil, errors.New("invalid file mode")
}

func OSSFini(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*OSSCtx)
	if !ok {
		return nil, errors.New("ctx must be *OSSCtx")
	}
	return nil, nil
}

func OSSUnlink(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	oss, ok := ctx.(*OSSCtx)
	if !ok {
		return nil, errors.New("ctx must be *OSSCtx")
	}
	_name, ok := param["name"]
	if !ok {
		return nil, errors.New("param.name miss")
	}
	name, ok := _name.(string)
	if !ok {
		return nil, errors.New("param.name msut be string")
	}
	fn := path.Join(oss.instance, oss.backupid, name)

	bucket, err := oss.client.Bucket(oss.bucket)
	if err != nil {
		return nil, errors.New("oss get bucket failed: " + err.Error())
	}

	err = bucket.DeleteObject(fn)
	if err != nil {
		return nil, errors.New("oss delete object failed: " + err.Error())
	}

	return nil, nil
}

func OSSEmpty(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*OSSCtx)
	if !ok {
		return nil, errors.New("ctx must be *OSSCtx")
	}
	return nil, nil
}

func OSSIsDir(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*OSSCtx)
	if !ok {
		return nil, errors.New("ctx must be *OSSCtx")
	}
	return false, nil
}

func OSSReaddir(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	ossctx, ok := ctx.(*OSSCtx)
	if !ok {
		return nil, errors.New("ctx must be *OSSCtx")
	}

	bucket, err := ossctx.client.Bucket(ossctx.bucket)
	if err != nil {
		return nil, errors.New("oss get bucket failed: " + err.Error())
	}

	files := make([]string, 0, 999)
	marker := ""
	for {
		lsRes, err := bucket.ListObjects(oss.Marker(marker))
		if err != nil {
			return nil, errors.New("oss list objects failed: " + err.Error())
		}

		for _, object := range lsRes.Objects {
			files = append(files, filepath.Base(object.Key))
		}

		if lsRes.IsTruncated {
			marker = lsRes.NextMarker
		} else {
			break
		}
	}

	return files, nil
}

func OSSRemoveAll(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	ossctx, ok := ctx.(*OSSCtx)
	if !ok {
		return nil, errors.New("ctx must be *OSSCtx")
	}

	bucket, err := ossctx.client.Bucket(ossctx.bucket)
	if err != nil {
		return nil, errors.New("oss get bucket failed: " + err.Error())
	}

	files := make([]string, 0, 999)
	marker := ""
	for {
		lsRes, err := bucket.ListObjects(oss.Marker(marker))
		if err != nil {
			return nil, errors.New("oss list objects failed: " + err.Error())
		}

		for _, object := range lsRes.Objects {
			files = append(files, object.Key)
		}

		if lsRes.IsTruncated {
			marker = lsRes.NextMarker
		} else {
			break
		}
	}

	for _, file := range files {
		err = bucket.DeleteObject(file)
		if err != nil {
			return nil, errors.New("oss delete object failed: " + err.Error())
		}
	}

	return nil, nil
}

func OSSGetPath(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	ossctx, ok := ctx.(*OSSCtx)
	if !ok {
		return nil, errors.New("ctx must be *OSSCtx")
	}

	return ossctx.path, nil
}

func ExportMapping(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	funcs := make(map[string]func(interface{}, map[string]interface{}) (interface{}, error))
	funcs["init"] = OSSInit
	funcs["fini"] = OSSFini
	funcs["mkdirs"] = OSSEmpty
	funcs["unlink"] = OSSUnlink
	funcs["open"] = OSSOpen
	funcs["create"] = OSSCreate
	funcs["close"] = OSSClose
	funcs["read"] = OSSRead
	funcs["write"] = OSSWrite
	funcs["chdir"] = OSSEmpty
	funcs["readdir"] = OSSReaddir
	funcs["readdir_r"] = OSSReaddir
	funcs["isdir"] = OSSIsDir
	funcs["rmdir"] = OSSRemoveAll
	funcs["size"] = OSSEmpty
	funcs["fallocate"] = OSSEmpty
	funcs["getpath"] = OSSGetPath
	funcs["exist"] = OSSExist
	return funcs, nil
}

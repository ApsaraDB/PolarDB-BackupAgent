package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"os"
	"path"
	"path/filepath"
	"time"
)

type S3Conf struct {
	Endpoint   string `json:"Endpoint"`
	ID         string `json:"ID"`
	Key        string `json:"Key"`
	Bucket     string `json:"Bucket"`
	InstanceID string `json:"InstanceID"`
	BackupID   string `json:"BackupID"`
	Secure     bool   `json:"Secure"`
}

const (
	S3_MODE_READ  = 1
	S3_MODE_WRITE = 2
)

type S3Ctx struct {
	bucket   string
	client   *minio.Client
	instance string
	backupid string
	path     string
}

type S3File struct {
	name   string
	mode   int
	r      *os.File
	w      *os.File
	stop   chan bool
	reader *minio.Object
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

func (file *S3File) Read(buf []byte) (int, error) {
	if file.mode != S3_MODE_READ {
		return 0, errors.New("only support read in download mode")
	}
	return file.reader.Read(buf)
}

func (file *S3File) Write(buf []byte) (int, error) {
	if file.mode != S3_MODE_WRITE {
		return 0, errors.New("only support write in upload mode")
	}
	return file.w.Write(buf)
}

func S3Init(ctx interface{}, param map[string]interface{}) (interface{}, error) {
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

	var initConf S3Conf
	err := json.Unmarshal(conf, &initConf)
	if err != nil {
		return nil, errors.New("parse plugin conf failed")
	}

	client, err := minio.New(initConf.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(initConf.ID, initConf.Key, ""),
		Secure: initConf.Secure,
	})
	if err != nil {
		return nil, err
	}

	_path := initConf.ID + ":" + initConf.Key + "@" + initConf.Endpoint + "/" + initConf.Bucket

	s3ctx := &S3Ctx{
		client:   client,
		bucket:   initConf.Bucket,
		instance: initConf.InstanceID,
		backupid: initConf.BackupID,
		path:     _path,
	}
	return s3ctx, nil
}

func S3Open(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	s3, ok := ctx.(*S3Ctx)
	if !ok {
		return nil, errors.New("ctx must be *S3Ctx")
	}
	_filepath, ok := param["name"]
	if !ok {
		return nil, errors.New("param.name miss")
	}

	filepath, ok := _filepath.(string)
	if !ok {
		return nil, errors.New("param.name msut be string")
	}

	name := path.Join(s3.instance, s3.backupid, filepath)
	object, err := s3.client.GetObject(context.Background(), s3.bucket, name, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	file := &S3File{
		mode:   S3_MODE_READ,
		name:   name,
		reader: object,
	}
	return file, nil
}

func S3Exist(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	s3, ok := ctx.(*S3Ctx)
	if !ok {
		return nil, errors.New("ctx must be *S3Ctx")
	}
	_filepath, ok := param["path"]
	if !ok {
		return nil, errors.New("param.path miss")
	}

	filepath, ok := _filepath.(string)
	if !ok {
		return nil, errors.New("param.path msut be string")
	}

	name := path.Join(s3.instance, s3.backupid, filepath)

	doesNotExist := "The specified key does not exist."
	tryMax := 3
	var err error
	for i := 0; i < tryMax; i++ {
		_, err = s3.client.StatObject(context.Background(), s3.bucket, name, minio.StatObjectOptions{})
		if err != nil {
			switch err.Error() {
			case doesNotExist:
				return false, nil
			default:
				// stat error, sleep and try again
				time.Sleep(5 * time.Second)
			}
		} else {
			return true, nil
		}
	}
	return false, errors.New("error stating object after try 3 times:" + err.Error())
}

func S3Create(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	s3, ok := ctx.(*S3Ctx)
	if !ok {
		return nil, errors.New("ctx must be *S3Ctx")
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

	name := path.Join(s3.instance, s3.backupid, filepath)
	file := &S3File{
		mode: S3_MODE_WRITE,
		name: name,
		r:    r,
		w:    w,
		stop: make(chan bool, 1),
	}
	go func() {
		reader := file.r
		_, err := s3.client.PutObject(context.Background(), s3.bucket, file.name, reader, -1, minio.PutObjectOptions{PartSize: 134217728})
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

func S3Write(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*S3Ctx)
	if !ok {
		return nil, errors.New("ctx must be *S3Ctx")
	}
	_file, ok := param["file"]
	if !ok {
		return nil, errors.New("param.file miss")
	}
	file, ok := _file.(*S3File)
	if !ok {
		return nil, errors.New("param.file msut be *S3File")
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

func S3Read(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*S3Ctx)
	if !ok {
		return nil, errors.New("ctx must be *S3Ctx")
	}
	_file, ok := param["file"]
	if !ok {
		return nil, errors.New("param.file miss")
	}
	file, ok := _file.(*S3File)
	if !ok {
		return nil, errors.New("param.file msut be *S3File")
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

func S3Close(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*S3Ctx)
	if !ok {
		return nil, errors.New("ctx must be *S3Ctx")
	}
	_file, ok := param["file"]
	if !ok {
		return nil, errors.New("param.file miss")
	}
	file, ok := _file.(*S3File)
	if !ok {
		return nil, errors.New("param.file msut be *S3File")
	}
	if file.mode == S3_MODE_WRITE {
		err := file.w.Close()
		if err != nil {
			return nil, err
		}
		_ = <-file.stop
		return nil, nil
	} else if file.mode == S3_MODE_READ {
		return nil, file.reader.Close()
	}
	return nil, errors.New("invalid file mode")
}

func S3Fini(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*S3Ctx)
	if !ok {
		return nil, errors.New("ctx must be *S3Ctx")
	}
	return nil, nil
}

func S3Unlink(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	s3, ok := ctx.(*S3Ctx)
	if !ok {
		return nil, errors.New("ctx must be *S3Ctx")
	}
	_name, ok := param["name"]
	if !ok {
		return nil, errors.New("param.name miss")
	}
	name, ok := _name.(string)
	if !ok {
		return nil, errors.New("param.name msut be string")
	}
	fn := path.Join(s3.instance, s3.backupid, name)
	return nil, s3.client.RemoveObject(context.Background(), s3.bucket, fn, minio.RemoveObjectOptions{})
}

func S3Empty(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*S3Ctx)
	if !ok {
		return nil, errors.New("ctx must be *S3Ctx")
	}
	return nil, nil
}

func S3IsDir(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*S3Ctx)
	if !ok {
		return nil, errors.New("ctx must be *S3Ctx")
	}
	return false, nil
}

func S3Readdir(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	s3ctx, ok := ctx.(*S3Ctx)
	if !ok {
		return nil, errors.New("ctx must be *S3Ctx")
	}
	objs := s3ctx.client.ListObjects(context.Background(), s3ctx.bucket, minio.ListObjectsOptions{
		Recursive: true,
		Prefix:    path.Join(s3ctx.instance, s3ctx.backupid),
	})
	files := make([]string, 0, 999)
	for obj := range objs {
		files = append(files, filepath.Base(obj.Key))
	}
	return files, nil
}

func S3RemoveAll(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	s3ctx, ok := ctx.(*S3Ctx)
	if !ok {
		return nil, errors.New("ctx must be *S3Ctx")
	}

	objs := s3ctx.client.ListObjects(context.Background(), s3ctx.bucket, minio.ListObjectsOptions{
		Recursive: true,
		Prefix:    path.Join(s3ctx.instance, s3ctx.backupid),
	})

	for obj := range objs {
		s3ctx.client.RemoveObject(context.Background(), s3ctx.bucket, obj.Key, minio.RemoveObjectOptions{
			GovernanceBypass: true,
		})
	}

	return nil, nil
}

func S3GetPath(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	s3ctx, ok := ctx.(*S3Ctx)
	if !ok {
		return nil, errors.New("ctx must be *S3Ctx")
	}

	return s3ctx.path, nil
}

func ExportMapping(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	funcs := make(map[string]func(interface{}, map[string]interface{}) (interface{}, error))
	funcs["init"] = S3Init
	funcs["fini"] = S3Fini
	funcs["mkdirs"] = S3Empty
	funcs["unlink"] = S3Unlink
	funcs["open"] = S3Open
	funcs["create"] = S3Create
	funcs["close"] = S3Close
	funcs["read"] = S3Read
	funcs["write"] = S3Write
	funcs["chdir"] = S3Empty
	funcs["readdir"] = S3Readdir
	funcs["readdir_r"] = S3Readdir
	funcs["isdir"] = S3IsDir
	funcs["rmdir"] = S3RemoveAll
	funcs["size"] = S3Empty
	funcs["fallocate"] = S3Empty
	funcs["getpath"] = S3GetPath
	funcs["exist"] = S3Exist
	return funcs, nil
}

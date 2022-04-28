package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

type LocalStorageInitConf struct {
	Path       string `json:"Path"`
	InstanceID string `json:"InstanceID"`
	BackupID   string `json:"BackupID"`
}

type StorageCtx struct {
	rootpath string
	path     string
}

type LocalFile struct {
	file *os.File
	mode int
}

const (
	_POSIX_FADV_NORMAL = iota
	_POSIX_FADV_RANDOM
	_POSIX_FADV_SEQUENTIAL
	_POSIX_FADV_WILLNEED
	_POSIX_FADV_DONTNEED
	_POSIX_FADV_NOREUSE
)

const (
	F_OK = 0 /* Test for existence.  */
)

var du_mutex sync.Mutex

func (file *LocalFile) Read(buf []byte) (int, error) {
	return file.file.Read(buf)
}

func (file *LocalFile) Write(buf []byte) (int, error) {
	return file.file.Write(buf)
}

func LocalStorageInit(ctx interface{}, param map[string]interface{}) (interface{}, error) {
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

	var initConf LocalStorageInitConf
	err := json.Unmarshal(conf, &initConf)
	if err != nil {
		return nil, errors.New("parse param.conf failed")
	}

	fullpath := path.Join(initConf.Path, initConf.InstanceID, initConf.BackupID)

	err = os.MkdirAll(fullpath, 0700)
	if err != nil {
		return nil, fmt.Errorf("storage path cannot be create: %s", err.Error())
	}

	sCtx := &StorageCtx{
		rootpath: initConf.Path,
		path:     fullpath,
	}
	return sCtx, nil
}

func LocalStorageFini(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	return nil, nil
}

func LocalStorageUnlink(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	sCtx, ok := ctx.(*StorageCtx)
	if !ok {
		return nil, errors.New("ctx must be *StorageCtx")
	}
	_filepath, ok := param["name"]
	if !ok {
		return nil, errors.New("param.name miss")
	}
	filepath, ok := _filepath.(string)
	if !ok {
		return nil, errors.New("param.name msut be string")
	}

	return nil, os.Remove(path.Join(sCtx.path, filepath))
}

func LocalStorageMkdirs(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	sCtx, ok := ctx.(*StorageCtx)
	if !ok {
		return nil, errors.New("ctx must be *StorageCtx")
	}
	_filepath, ok := param["path"]
	if !ok {
		return nil, errors.New("param.path miss")
	}
	filepath, ok := _filepath.(string)
	if !ok {
		return nil, errors.New("param.path msut be string")
	}

	_mode, ok := param["mode"]
	if !ok {
		return nil, errors.New("param.mode miss")
	}
	mode, ok := _mode.(uint)
	if !ok {
		return nil, errors.New("param.mode must be uint32")
	}

	return nil, os.MkdirAll(path.Join(sCtx.path, filepath), os.FileMode(mode))
}

func LocalStorageOpen(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	sCtx, ok := ctx.(*StorageCtx)
	if !ok {
		return nil, errors.New("ctx must be *StorageCtx")
	}
	_filepath, ok := param["name"]
	if !ok {
		return nil, errors.New("param.name miss")
	}
	filepath, ok := _filepath.(string)
	if !ok {
		return nil, errors.New("param.name msut be string")
	}

	_file, err := os.OpenFile(path.Join(sCtx.path, filepath), os.O_RDWR, os.ModePerm)
	file := &LocalFile{
		file: _file,
		mode: os.O_RDWR,
	}
	return file, err
}

func LocalStorageCreate(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	sCtx, ok := ctx.(*StorageCtx)
	if !ok {
		return nil, errors.New("ctx must be *StorageCtx")
	}
	_filepath, ok := param["name"]
	if !ok {
		return nil, errors.New("param.name miss")
	}
	filepath, ok := _filepath.(string)
	if !ok {
		return nil, errors.New("param.name msut be string")
	}
	_file, err := os.Create(path.Join(sCtx.path, filepath))
	file := &LocalFile{
		file: _file,
		mode: os.O_CREATE,
	}
	return file, err
}

func LocalStorageRead(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*StorageCtx)
	if !ok {
		return nil, errors.New("ctx must be *StorageCtx")
	}

	_file, ok := param["file"]
	if !ok {
		return nil, errors.New("param.file miss")
	}
	file, ok := _file.(*LocalFile)
	if !ok {
		return nil, errors.New("param.file msut be *os.File")
	}

	_buf, ok := param["buf"]
	if !ok {
		return nil, errors.New("param.buf miss")
	}
	buf, ok := _buf.([]byte)
	if !ok {
		return nil, errors.New("param.file msut be *os.File")
	}

	return file.file.Read(buf)
}

func LocalStorageWrite(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*StorageCtx)
	if !ok {
		return nil, errors.New("ctx must be *StorageCtx")
	}

	_file, ok := param["file"]
	if !ok {
		return nil, errors.New("param.file miss")
	}
	file, ok := _file.(*LocalFile)
	if !ok {
		return nil, errors.New("param.file msut be *os.File")
	}

	_buf, ok := param["buf"]
	if !ok {
		return nil, errors.New("param.buf miss")
	}
	buf, ok := _buf.([]byte)
	if !ok {
		return nil, errors.New("param.file msut be *os.File")
	}

	return file.file.Write(buf)
}

func LocalStorageClose(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*StorageCtx)
	if !ok {
		return nil, errors.New("ctx must be *StorageCtx")
	}

	_file, ok := param["file"]
	if !ok {
		return nil, errors.New("param.file miss")
	}
	file, ok := _file.(*LocalFile)
	if !ok {
		return nil, errors.New("param.file msut be *os.File")
	}

	if file.mode == os.O_CREATE {
		file.file.Sync()
	}

	unix.Fadvise(int(file.file.Fd()), 0, 0, _POSIX_FADV_DONTNEED)

	return nil, file.file.Close()
}

func LocalStorageTruncate(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*StorageCtx)
	if !ok {
		return nil, errors.New("ctx must be *StorageCtx")
	}

	_file, ok := param["file"]
	if !ok {
		return nil, errors.New("param.file miss")
	}

	file, ok := _file.(*LocalFile)
	if !ok {
		return nil, errors.New("param.file msut be *LocalFile")
	}

	_size, ok := param["size"]
	if !ok {
		return nil, errors.New("param.size must exist")
	}

	length, ok := _size.(int64)
	if !ok {
		return nil, errors.New("param length should be int64")
	}

	err := file.file.Truncate(length)
	return nil, err
}

func LocalStorageSeek(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*StorageCtx)
	if !ok {
		return nil, errors.New("ctx must be *StorageCtx")
	}

	_file, ok := param["file"]
	if !ok {
		return nil, errors.New("param.file miss")
	}

	file, ok := _file.(*LocalFile)
	if !ok {
		return nil, errors.New("param.file msut be *os.File")
	}

	_offset, ok := param["offset"]
	if !ok {
		return nil, errors.New("param.offset miss")
	}

	offset, ok := _offset.(int)
	if !ok {
		return nil, errors.New("param.offset must be int")
	}

	whence := 0
	n_off, err := file.file.Seek(int64(offset), whence)
	return int(n_off), err
}

func LocalStorageFallocate(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*StorageCtx)
	if !ok {
		return nil, errors.New("ctx must be *StorageCtx")
	}

	_file, ok := param["file"]
	if !ok {
		return nil, errors.New("param.file miss")
	}

	file, ok := _file.(*LocalFile)
	if !ok {
		return nil, errors.New("param.file msut be *os.File")
	}

	var offset int64
	_offset, ok := param["offset"]
	if !ok {
		offset = int64(0)
	} else {
		offset, ok = _offset.(int64)
		if !ok {
			return nil, errors.New("param offset should be int64")
		}
	}

	_size, ok := param["size"]
	if !ok {
		return nil, errors.New("param.size must exist")
	}

	length, ok := _size.(int64)
	if !ok {
		return nil, errors.New("param length should be int64")
	}

	return nil, syscall.Fallocate(int(file.file.Fd()), uint32(0), int64(offset), length)
}

func LocalStorageAccess(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	sCtx, ok := ctx.(*StorageCtx)
	if !ok {
		return nil, errors.New("ctx must be *StorageCtx")
	}
	_filepath, ok := param["path"]
	if !ok {
		return nil, errors.New("param.path miss")
	}
	filepath, ok := _filepath.(string)
	if !ok {
		return nil, errors.New("param.path msut be string")
	}

	_mode, ok := param["mode"]
	if !ok {
		return nil, errors.New("param.mode miss")
	}
	mode, ok := _mode.(int)
	if !ok {
		return nil, errors.New("param.mode must be int")
	}
	if mode != F_OK {
		return nil, errors.New("param.mode of fs access only support F_OK now")
	}

	_, err := os.Stat(path.Join(sCtx.path, filepath))
	if os.IsNotExist(err) {
		return -1, nil
	}
	return 0, err
}

func LocalStorageExist(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	sCtx, ok := ctx.(*StorageCtx)
	if !ok {
		return nil, errors.New("ctx must be *StorageCtx")
	}
	_filepath, ok := param["path"]
	if !ok {
		return nil, errors.New("param.path miss")
	}
	filepath, ok := _filepath.(string)
	if !ok {
		return nil, errors.New("param.path msut be string")
	}

	_, err := os.Stat(path.Join(sCtx.path, filepath))
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func LocalStorageDu(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	sCtx, ok := ctx.(*StorageCtx)
	if !ok {
		return nil, errors.New("ctx must be *StorageCtx")
	}

	_path, ok := param["path"]
	if !ok {
		return nil, errors.New("param.path miss")
	}
	p, ok := _path.(string)
	if !ok {
		return nil, errors.New("param.string must be string")
	}

	du_mutex.Lock()
	defer du_mutex.Unlock()

	fscmd := `du ` + sCtx.rootpath + p + ` |tail -1 | awk -F '\t' '{printf "%s", $1}'`
	cmd := exec.Command("/bin/bash", "-c", fscmd)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, errors.New("can not obtain stdout pipe for command: " + err.Error())
	}

	if err := cmd.Start(); err != nil {
		return nil, errors.New("command is err when execute: " + err.Error())
	}

	bytes, err := ioutil.ReadAll(stdout)
	if err != nil {
		return nil, errors.New("ReadAll Stdout failed: " + err.Error())
	}

	if err := cmd.Wait(); err != nil {
		return nil, errors.New("cmd wait failed: " + err.Error())
	}

	size_kb, err := strconv.Atoi(string(bytes))
	if err != nil {
		return nil, errors.New("conv failed: " + err.Error())
	}

	size_b := size_kb * 1024

	return int64(size_b), nil
}

func LocalStorageReaddir(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	sCtx, ok := ctx.(*StorageCtx)
	if !ok {
		return nil, errors.New("ctx must be *StorageCtx")
	}

	_path, ok := param["path"]
	if !ok {
		return nil, errors.New("param.path miss")
	}

	dir, ok := _path.(string)
	if !ok {
		return nil, errors.New("param.path must be string")
	}

	file, err := os.Open(path.Join(sCtx.path, dir))
	if err != nil {
		return nil, err
	}

	return file.Readdirnames(-1)
}

func LocalStorageRename(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	sCtx, ok := ctx.(*StorageCtx)
	if !ok {
		return nil, errors.New("ctx must be *StorageCtx")
	}

	_source, ok := param["source"]
	if !ok {
		return nil, errors.New("param.path miss")
	}

	source, ok := _source.(string)
	if !ok {
		return nil, errors.New("param.path must be string")
	}

	_dest, ok := param["dest"]
	if !ok {
		return nil, errors.New("param.path miss")
	}

	dest, ok := _dest.(string)
	if !ok {
		return nil, errors.New("param.path must be string")
	}

	// fmt.Printf("ctx path[%s]. dir[%s]\n", sCtx.path, dir)

	return nil, os.Rename(path.Join(sCtx.path, source), path.Join(sCtx.path, dest))
}

func LocalStorageReaddirRecursive(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	sCtx, ok := ctx.(*StorageCtx)
	if !ok {
		return nil, errors.New("ctx must be *StorageCtx")
	}

	_path, ok := param["path"]
	if !ok {
		return nil, errors.New("param.path miss")
	}

	dirpath, ok := _path.(string)
	if !ok {
		return nil, errors.New("param.path must be string")
	}

	files := make([]string, 0, 1024)
	err := filepath.Walk(path.Join(sCtx.path, dirpath), func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		files = append(files, p)
		return nil
	})
	if err != nil {
		return nil, err
	}

	fns := make([]string, 0, 4096)
	for _, fn := range files {
		fn = strings.Replace(fn, sCtx.path, "", 1)
		fn = strings.TrimLeft(fn, "/")
		fns = append(fns, fn)
	}
	return fns, nil
}

func LocalStorageChdir(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*StorageCtx)
	if !ok {
		return nil, errors.New("ctx must be *StorageCtx")
	}

	_filepath, ok := param["path"]
	if !ok {
		return nil, errors.New("param.path miss")
	}

	filepath, ok := _filepath.(string)
	if !ok {
		return nil, errors.New("param.path must be string")
	}

	return nil, os.Chdir(filepath)
}

func LocalStorageIsDir(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	sCtx, ok := ctx.(*StorageCtx)
	if !ok {
		return nil, errors.New("ctx must be *StorageCtx")
	}

	_path, ok := param["path"]
	if !ok {
		return nil, errors.New("param.path miss")
	}
	p, ok := _path.(string)
	if !ok {
		return nil, errors.New("param.path must be string")
	}

	fi, err := os.Stat(path.Join(sCtx.path, p))
	if err != nil {
		return nil, err
	}
	status := fi.Mode().IsDir()
	return status, nil
}

func LocalStorageSize(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	sCtx, ok := ctx.(*StorageCtx)
	if !ok {
		return nil, errors.New("ctx must be *StorageCtx")
	}

	_name, ok := param["name"]
	if !ok {
		return nil, errors.New("param.name miss")
	}
	name, ok := _name.(string)
	if !ok {
		return nil, errors.New("param.name must be string")
	}

	fi, err := os.Stat(path.Join(sCtx.path, name))
	if err != nil {
		return nil, err
	}
	return fi.Size(), nil
}

func LocalStorageRemoveAll(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	sCtx, ok := ctx.(*StorageCtx)
	if !ok {
		return nil, errors.New("ctx must be *StorageCtx")
	}

	_dirpath, ok := param["path"]
	if !ok {
		return nil, errors.New("param.path miss")
	}
	dirpath, ok := _dirpath.(string)
	if !ok {
		return nil, errors.New("param.path must be string")
	}

	return nil, os.RemoveAll(path.Join(sCtx.path, dirpath))
}

func LocalStorageGetPath(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	sCtx, ok := ctx.(*StorageCtx)
	if !ok {
		return nil, errors.New("ctx must be *StorageCtx")
	}

	return sCtx.rootpath, nil
}

func LocalStorageEmpty(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	return nil, nil
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

func ExportMapping(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	funcs := make(map[string]func(interface{}, map[string]interface{}) (interface{}, error))
	funcs["init"] = LocalStorageInit
	funcs["fini"] = LocalStorageFini
	funcs["mkdirs"] = LocalStorageMkdirs
	funcs["unlink"] = LocalStorageUnlink
	funcs["open"] = LocalStorageOpen
	funcs["create"] = LocalStorageCreate
	funcs["close"] = LocalStorageClose
	funcs["read"] = LocalStorageRead
	funcs["write"] = LocalStorageWrite
	funcs["rmdir"] = LocalStorageRemoveAll
	funcs["chdir"] = LocalStorageChdir
	funcs["readdir_r"] = LocalStorageReaddirRecursive
	funcs["readdir"] = LocalStorageReaddir
	funcs["size"] = LocalStorageSize
	funcs["isdir"] = LocalStorageIsDir
	funcs["rename"] = LocalStorageRename
	funcs["fallocate"] = LocalStorageEmpty
	funcs["access"] = LocalStorageAccess
	funcs["getpath"] = LocalStorageGetPath
	funcs["exist"] = LocalStorageExist
	funcs["du"] = LocalStorageDu
	funcs["truncate"] = LocalStorageTruncate
	funcs["seek"] = LocalStorageSeek
	return funcs, nil
}

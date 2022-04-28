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
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"unsafe"
)

const (
	PFS_MODE_CHUNK = 1
	PFS_MODE_FILE  = 2
)

const (
	PFS_RW_MODE = 1
	PFS_RO_MODE = 2
)

const (
	FALLOC_FL_NO_HIDE_STALE = 4
)

type PFSInitConf struct {
	Pbd     string `json:"Pbd"`
	Cluster string `json:"Cluster"`
	// chunk, file
	Mode string `json:"Mode"`
	// backup, recovery
	Flags    string `json:"Flags"`
	Name     string `json:"Name"`
	DBPath   string `json:"DBPath"`
	LogPath  string `json:"LogPath"`
	GuessDev bool   `json:"GuessDev"`
}

type PFSChunkStream struct {
	file *C.pfs_chunkstream_t
}

type PFSFile struct {
	fd int
}

type PFSChunkMeta struct {
	meta *C.pfs_chunkstream_desc_t
}

var romap sync.Map
var rwmap sync.Map
var rs_mutex sync.Mutex // pfs resource
var du_mutex sync.Mutex

type PFSCtx struct {
	mode   int
	fd     int
	pbd    string
	prefix string
	dbpath string
	meta   *PFSChunkMeta
	action int
	count  int32
}

/*
 * If prefix of dev is mapper_, we regard it as a valid dev name.
 * Otherwise, it may be the origin wwid and we try add prefix such as mapper or mapper_pv-
 * to find the correct dev name for pfs.
 */
func getPBD(dev string) (string, error) {
	if strings.HasPrefix(dev, "mapper_") {
		return dev, nil
	}

	_, err := os.Stat(path.Join("/dev/", dev))
	if err == nil {
		return dev, nil
	}

	_, err = os.Stat(path.Join("/dev/mapper/", dev))
	if err == nil {
		return "mapper_" + dev, nil
	}
	_, err = os.Stat(path.Join("/dev/mapper/", "pv-"+dev))
	if err == nil {
		return "mapper_pv-" + dev, nil
	}
	return "", fmt.Errorf("cannot find match pbd for: %s", dev)
}

// param:
//    conf []byte
func PFSInit(ctx interface{}, param map[string]interface{}) (interface{}, error) {
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

	var initConf PFSInitConf
	err := json.Unmarshal(conf, &initConf)
	if err != nil {
		return nil, errors.New("parse plugin conf failed")
	}

	if initConf.Flags != "backup" && initConf.Flags != "restore" {
		return nil, errors.New("flags muse be `backup` or `restore`")
	}

	if initConf.GuessDev {
		initConf.Pbd, err = getPBD(initConf.Pbd)
		if err != nil {
			return nil, err
		}
	}

	logpath := "/tmp/" + initConf.Pbd + "-" + initConf.Mode + "-" + initConf.Flags + ".log"
	if initConf.LogPath != "" {
		logpath = initConf.LogPath
	}

	if initConf.Mode == "chunk" {
		flags := 0x2
		if initConf.Flags == "backup" {
			flags = 0x1 | 0x10
		}
		m, err := _PFSChunkMetaInit(initConf.Cluster, initConf.Pbd, uint(flags))
		if err != nil {
			return nil, err
		}
		pfsctx := &PFSCtx{
			mode:  PFS_MODE_CHUNK,
			meta:  m,
			pbd:   initConf.Pbd,
			count: 1,
		}
		return pfsctx, nil
	} else if initConf.Mode == "file" {
		rs_mutex.Lock()
		defer rs_mutex.Unlock()

		action := PFS_RW_MODE
		pfsmap := &rwmap
		flags := 0x1000 | 0x0002 | 0x0010 | 0x0001
		if initConf.Flags == "backup" {
			action = PFS_RO_MODE
			pfsmap = &romap
			flags = 0x1000 | 0x0001 | 0x0010
		}

		_ins, ok := pfsmap.Load(initConf.Pbd)
		if ok {
			ins, ok := _ins.(*PFSCtx)
			if !ok {
				return nil, errors.New("invalid pfsctx")
			}
			ins.count++
			return ins, nil
		}

		err := _PFSMount(initConf.Cluster, initConf.Pbd, flags)
		if err != nil {
			return nil, err
		}

		fd, err := syscall.Open(logpath, syscall.O_CREAT|syscall.O_WRONLY|syscall.O_APPEND|syscall.O_SYNC, 0644)
		if err != nil {
			_PFSUmount(initConf.Pbd)
			return nil, fmt.Errorf("init pfs log failed: %s", err.Error())
		}
		err = syscall.Dup3(fd, 2, 0)
		if err != nil {
			_PFSUmount(initConf.Pbd)
			return nil, errors.New("redirect pfs log failed")
		}

		pfsctx := &PFSCtx{
			mode:   PFS_MODE_FILE,
			pbd:    initConf.Pbd,
			dbpath: initConf.DBPath,
			prefix: path.Join("/", initConf.Pbd, initConf.DBPath),
			count:  1,
			action: action,
			fd:     fd,
		}

		if initConf.Flags == "restore" {
			err = _PFSFileMkdir(initConf.Pbd, pfsctx.prefix, 0700)
			if err == nil || err == syscall.EEXIST {
				pfsmap.Store(initConf.Pbd, pfsctx)
				return pfsctx, nil
			}
			_PFSUmount(initConf.Pbd)
			return nil, err
		}
		pfsmap.Store(initConf.Pbd, pfsctx)
		return pfsctx, nil
	} else {
		return nil, fmt.Errorf("ctx.mode must be `chunk` or `file`")
	}
	return nil, nil
}

func PFSFini(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	if pfsctx.mode == PFS_MODE_FILE {
		rs_mutex.Lock()
		defer rs_mutex.Unlock()

		if pfsctx.count <= 0 {
			return nil, errors.New("invalid pfsctx refernece")
		}

		pfsctx.count--
		if pfsctx.count > 0 {
			return nil, nil
		}
		if pfsctx.action == PFS_RO_MODE {
			romap.Delete(pfsctx.pbd)
		}
		if pfsctx.action == PFS_RW_MODE {
			rwmap.Delete(pfsctx.pbd)
		}
		return PFSFileFini(pfsctx, param)
	} else if pfsctx.mode == PFS_MODE_CHUNK {
		return PFSChunkFini(pfsctx.meta, param)
	}
	if pfsctx.fd > 0 {
		syscall.Close(pfsctx.fd)
	}
	return nil, errors.New("ctx.mode must be `chunk` or `file`")
}

// flags use PFS_TOOL|PFS_RD
func _PFSMount(cluster, pbd string, flags int) error {
	ret, err := C.pfs_mount(C.CString(cluster), C.CString(pbd), 0, C.int(flags))
	if int(ret) != 0 {
		return err
	}
	return nil
}

func _PFSUmount(pbd string) error {
	status, err := C.pfs_umount(C.CString(pbd))
	if status < 0 {
		return err
	}
	return nil
}

func PFSFileFini(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	return nil, _PFSUmount(pfsctx.pbd)
}

func _PFSFileMkdir(pbd, dir string, mode uint) error {
	ret, err := C.pfs_mkdir(C.CString(dir), C.uint(mode))
	if ret < 0 {
		return err
	}
	return nil
}

func _PFSFileMkdirAll(pbd, path string, mode uint) error {
	var stat C.struct_stat
	ret, err := C.pfs_stat(C.CString(path), &stat)
	if ret == 0 {
		// is dir
		if (stat.st_mode & 00170000) == 0040000 {
			return nil
		}
		return fmt.Errorf("file already exists: %s", path)
	}

	i := len(path)
	for i > 0 && path[i-1] == '/' {
		i--
	}

	j := i
	for j > 0 && !(path[j-1] == '/') {
		j--
	}

	if j > 1 {
		err = _PFSFileMkdirAll(pbd, path[:j-1], mode)
		if err != nil {
			return err
		}
	}

	err = _PFSFileMkdir(pbd, path, mode)
	if err != nil {
		ret, err2 := _PFSIsDir(path)
		if err2 == nil && ret {
			return nil
		}
		return err
	}
	return nil
}

func PFSMkdirs(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	if pfsctx.mode == PFS_MODE_FILE {
		_mode, ok := param["mode"]
		if !ok {
			return nil, errors.New("param.mode miss")
		}
		mode, ok := _mode.(uint)
		if !ok {
			return nil, errors.New("param.mode must be uint")
		}
		_path, ok := param["path"]
		if !ok {
			return nil, errors.New("param.path miss")
		}
		p, ok := _path.(string)
		if !ok {
			return nil, errors.New("param.string must be string")
		}
		p = path.Join(pfsctx.prefix, p)
		return nil, _PFSFileMkdirAll(pfsctx.pbd, p, mode)
	} else if pfsctx.mode == PFS_MODE_CHUNK {
		return nil, nil
	}
	return nil, errors.New("ctx.mode must be `chunk` or `file`")
}

func _PFSAccess(pbd, dir string, mode int) (int, error) {
	ret, err := C.pfs_access(C.CString(dir), C.int(mode))
	if ret > 0 {
		return int(ret), err
	}
	return int(ret), nil
}

func PFSAccess(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	if pfsctx.mode == PFS_MODE_FILE {
		_mode, ok := param["mode"]
		if !ok {
			return nil, errors.New("param.mode miss")
		}
		mode, ok := _mode.(int)
		if !ok {
			return nil, errors.New("param.mode must be int")
		}
		_path, ok := param["path"]
		if !ok {
			return nil, errors.New("param.path miss")
		}
		p, ok := _path.(string)
		if !ok {
			return nil, errors.New("param.string must be string")
		}
		p = path.Join(pfsctx.prefix, p)
		return _PFSAccess(pfsctx.pbd, p, mode)
	} else if pfsctx.mode == PFS_MODE_CHUNK {
		return nil, nil
	}
	return nil, errors.New("ctx.mode must be `chunk` or `file`")
}

func _PFSFileExist(path string) (bool, error) {
	var stat C.struct_stat
	ret, err := C.pfs_stat(C.CString(path), &stat)
	if ret < 0 {
		if err.Error() == "no such file or directory" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func PFSExist(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}
	if pfsctx.mode == PFS_MODE_FILE {
		_path, ok := param["path"]
		if !ok {
			return nil, errors.New("param.path miss")
		}
		p, ok := _path.(string)
		if !ok {
			return nil, errors.New("param.string must be string")
		}
		fullpath := path.Join(pfsctx.prefix, p)
		return _PFSFileExist(fullpath)
	} else if pfsctx.mode == PFS_MODE_CHUNK {
		// XXX, really need a file exists api in chunk mode?
		return true, nil
	}
	return nil, errors.New("ctx.mode must be `chunk` or `file`")
}

func PFSDu(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
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

	pfscmd := `/usr/local/bin/pfs -C disk du /` + pfsctx.pbd + `/data` + p + ` |tail -1 | awk -F '\t' '{printf "%s", $1}'`
	cmd := exec.Command("/bin/bash", "-c", pfscmd)

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

func _PFSIsDir(path string) (bool, error) {
	var stat C.struct_stat
	ret, err := C.pfs_stat(C.CString(path), &stat)
	if ret < 0 {
		return false, err
	}
	return (stat.st_mode & 00170000) == 0040000, nil
}

func _PFSFileSize(path string) (int64, error) {
	var stat C.struct_stat
	ret, err := C.pfs_stat(C.CString(path), &stat)
	if ret < 0 {
		return 0, err
	}
	return int64(stat.st_size), nil
}

func _PFSRename(src string, dest string) error {
	// what if ret < 0 while err is nil
	_, err := C.pfs_rename(C.CString(src), C.CString(dest))
	return err
}

func PFSFileIsDir(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	_name, ok := param["path"]
	if !ok {
		return nil, errors.New("param.path miss")
	}
	name, ok := _name.(string)
	if !ok {
		return nil, errors.New("param.path must be string")
	}

	name = path.Join(pfsctx.prefix, name)
	return _PFSIsDir(name)
}

func PFSFileSize(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	_name, ok := param["name"]
	if !ok {
		return nil, errors.New("param.name miss")
	}
	name, ok := _name.(string)
	if !ok {
		return nil, errors.New("param.name must be string")
	}
	name = path.Join(pfsctx.prefix, name)
	return _PFSFileSize(name)
}

func PFSIsDir(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	if pfsctx.mode == PFS_MODE_FILE {
		return PFSFileIsDir(pfsctx, param)
	} else if pfsctx.mode == PFS_MODE_CHUNK {
		return false, nil
	}
	return nil, errors.New("ctx.mode must be `chunk` or `file`")
}

func PFSSize(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}
	if pfsctx.mode == PFS_MODE_FILE {
		return PFSFileSize(pfsctx, param)
	} else if pfsctx.mode == PFS_MODE_CHUNK {
		return 0, nil
	}
	return nil, errors.New("ctx.mode must be `chunk` or `file`")
}

func PFSRename(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}
	if pfsctx.mode == PFS_MODE_FILE {
		return nil, _PFSRename(param["source"].(string), param["dest"].(string))
	} else if pfsctx.mode == PFS_MODE_CHUNK {
		return nil, errors.New("Not Implemented")
	}
	return nil, errors.New("ctx.mode must be `chunk` or `file`")
}

func _PFSFileOpen(name string, flags, mode uint) (*PFSFile, error) {
	fd, err := C.pfs_open(C.CString(name), C.int(flags), C.uint(mode))
	if fd < 0 {
		return nil, err
	}
	return &PFSFile{fd: int(fd)}, nil
}

func PFSFileOpen(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}
	_name, ok := param["name"]
	if !ok {
		return nil, errors.New("param.name miss")
	}
	name, ok := _name.(string)
	if !ok {
		return nil, errors.New("param.name must be string")
	}

	_flags, ok := param["flags"]
	if !ok {
		return nil, errors.New("param.flags miss")
	}
	flags, ok := _flags.(uint)
	if !ok {
		return nil, errors.New("param.flags must be uint")
	}

	_mode, ok := param["mode"]
	if !ok {
		return nil, errors.New("param.mode miss")
	}
	mode, ok := _mode.(uint)
	if !ok {
		return nil, errors.New("param.mode must be uint")
	}

	name = path.Join(pfsctx.prefix, name)
	return _PFSFileOpen(name, flags, mode)
}

func PFSOpen(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	if pfsctx.mode == PFS_MODE_FILE {
		return PFSFileOpen(pfsctx, param)
	} else if pfsctx.mode == PFS_MODE_CHUNK {
		return PFSChunkOpen(pfsctx.meta, param)
	}
	return nil, fmt.Errorf("ctx.mode must be `chunk` or `file`")
}

func _PFSFileCreate(name string, mode uint) (*PFSFile, error) {
	fd, err := C.pfs_open(C.CString(name), syscall.O_CREAT|syscall.O_TRUNC, C.uint(mode))
	if fd < 0 {
		return nil, err
	}
	return &PFSFile{fd: int(fd)}, nil
}

func PFSFileCreate(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	_name, ok := param["name"]
	if !ok {
		return nil, errors.New("param.name miss")
	}
	name, ok := _name.(string)
	if !ok {
		return nil, errors.New("param.name must be string")
	}

	_mode, ok := param["mode"]
	if !ok {
		return nil, errors.New("param.mode miss")
	}
	mode, ok := _mode.(uint)
	if !ok {
		return nil, errors.New("param.mode must be uint")
	}
	name = path.Join(pfsctx.prefix, name)
	return _PFSFileCreate(name, mode)
}

func PFSCreate(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	if pfsctx.mode == PFS_MODE_FILE {
		return PFSFileCreate(pfsctx, param)
	} else if pfsctx.mode == PFS_MODE_CHUNK {
		return PFSChunkCreate(pfsctx.meta, param)
	}
	return nil, fmt.Errorf("ctx.mode must be `chunk` or `file`")
}

func _PFSFileFallocate(fd int, offet int64, len int64) error {
	ret, err := C.pfs_fallocate(C.int(fd), FALLOC_FL_NO_HIDE_STALE, C.long(offet), C.long(len))
	if ret != 0 {
		return errors.New("pfs file fallocate failed: " + err.Error())
	}
	return nil
}

func PFSFileFallocate(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	_file, ok := param["file"]
	if !ok {
		return nil, errors.New("param.file miss")
	}

	file, ok := _file.(*PFSFile)
	if !ok {
		return nil, errors.New("param.file msut be *PFSFile")
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

	return nil, _PFSFileFallocate(file.fd, offset, length)
}

func PFSChunkFallocate(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	return nil, fmt.Errorf("not support fallocate in chunk yet")
}

func PFSFallocate(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	if pfsctx.mode == PFS_MODE_FILE {
		return PFSFileFallocate(pfsctx, param)
	} else if pfsctx.mode == PFS_MODE_CHUNK {
		return PFSChunkFallocate(pfsctx.meta, param)
	}
	return nil, fmt.Errorf("ctx.mode must be `chunk` or `file`")
}

func _PFSFileTruncate(fd int, len int64) error {
	ret, err := C.pfs_ftruncate(C.int(fd), C.long(len))
	if ret != 0 {
		return errors.New("pfs file truncate failed: " + err.Error())
	}
	return nil
}

func PFSFileTruncate(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	_file, ok := param["file"]
	if !ok {
		return nil, errors.New("param.file miss")
	}

	file, ok := _file.(*PFSFile)
	if !ok {
		return nil, errors.New("param.file msut be *PFSFile")
	}

	_size, ok := param["size"]
	if !ok {
		return nil, errors.New("param.size must exist")
	}

	length, ok := _size.(int64)
	if !ok {
		return nil, errors.New("param length should be int64")
	}

	return nil, _PFSFileTruncate(file.fd, length)
}

func PFSChunkTruncate(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	return nil, fmt.Errorf("not support truncate in chunk yet")
}

func PFSTruncate(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	if pfsctx.mode == PFS_MODE_FILE {
		return PFSFileTruncate(pfsctx, param)
	} else if pfsctx.mode == PFS_MODE_CHUNK {
		return PFSChunkTruncate(pfsctx.meta, param)
	}
	return nil, fmt.Errorf("ctx.mode must be `chunk` or `file`")
}

func _PFSFileClose(fd int) error {
	ret, err := C.pfs_close(C.int(fd))
	if ret < 0 {
		return err
	}
	return nil
}

func PFSFileClose(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	_pfsfile, ok := param["file"]
	if !ok {
		return nil, errors.New("param.file miss")
	}

	pfsfile, ok := _pfsfile.(*PFSFile)
	if !ok {
		return nil, errors.New("param.file must be PFSFile")
	}

	file := pfsfile.fd

	return nil, _PFSFileClose(file)
}

func PFSClose(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	if pfsctx.mode == PFS_MODE_FILE {
		return PFSFileClose(pfsctx, param)
	} else if pfsctx.mode == PFS_MODE_CHUNK {
		return PFSChunkClose(pfsctx.meta, param)
	}
	return nil, fmt.Errorf("ctx.mode must be `chunk` or `file`")
}

func _PFSFileSeek(fd int, offset int) (int, error) {
	whence := 0
	n_off, err := C.pfs_lseek(C.int(fd), C.long(offset), C.int(whence))
	return int(n_off), err
}

func PFSFileSeek(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	_pfsfile, ok := param["file"]
	if !ok {
		return nil, errors.New("param.file miss")
	}

	pfsfile, ok := _pfsfile.(*PFSFile)
	if !ok {
		return nil, errors.New("param.file must be PFSFile")
	}

	fd := pfsfile.fd

	_offset, ok := param["offset"]
	if !ok {
		return nil, errors.New("param.offset miss")
	}

	offset, ok := _offset.(int)
	if !ok {
		return nil, errors.New("param.offset must be int")
	}

	return _PFSFileSeek(fd, offset)
}

func PFSSeek(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	if pfsctx.mode == PFS_MODE_FILE {
		return PFSFileSeek(pfsctx, param)
	} else if pfsctx.mode == PFS_MODE_CHUNK {
		return nil, fmt.Errorf("not support seek in chunk mode yet")
	}
	return nil, fmt.Errorf("ctx.mode must be `chunk` or `file`")
}

func _PFSFileRead(fd int, buf []byte) (int, error) {
	c_buf := unsafe.Pointer(&buf[0])
	c_buf_len := C.ulong(len(buf))
	size, err := C.pfs_read(C.int(fd), c_buf, c_buf_len)
	if size < 0 {
		return 0, err
	} else if size == 0 {
		return 0, io.EOF
	}
	return int(size), nil
}

func PFSFileRead(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	_fd, ok := param["file"]
	if !ok {
		return nil, errors.New("param.file miss")
	}
	fd, ok := _fd.(int)
	if !ok {
		return nil, errors.New("param.file must be int")
	}

	_buf, ok := param["buf"]
	if !ok {
		return nil, errors.New("param.buf miss")
	}
	buf, ok := _buf.([]byte)
	if !ok {
		return nil, errors.New("param.buf must be []byte")
	}

	return _PFSFileRead(fd, buf)
}

func (f *PFSFile) Read(buf []byte) (int, error) {
	return _PFSFileRead(f.fd, buf)
}

func (f *PFSFile) Write(buf []byte) (int, error) {
	return _PFSFileWrite(f.fd, buf)
}

func PFSRead(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	if pfsctx.mode == PFS_MODE_FILE {
		return PFSFileRead(pfsctx, param)
	} else if pfsctx.mode == PFS_MODE_CHUNK {
		return PFSChunkRead(pfsctx.meta, param)
	}
	return nil, fmt.Errorf("ctx.mode must be `chunk` or `file`")
}

func _PFSFileWrite(fd int, buf []byte) (int, error) {
	c_buf := unsafe.Pointer(&buf[0])
	c_buf_len := C.ulong(len(buf))
	size, err := C.pfs_write(C.int(fd), c_buf, c_buf_len)
	if size < 0 {
		return 0, err
	}
	return int(size), nil
}

func PFSFileWrite(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	_fd, ok := param["file"]
	if !ok {
		return nil, errors.New("param.file miss")
	}
	fd, ok := _fd.(int)
	if !ok {
		return nil, errors.New("param.file must be int")
	}

	_buf, ok := param["buf"]
	if !ok {
		return nil, errors.New("param.buf miss")
	}
	buf, ok := _buf.([]byte)
	if !ok {
		return nil, errors.New("param.buf must be []byte")
	}

	return _PFSFileWrite(fd, buf)
}

func PFSWrite(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	if pfsctx.mode == PFS_MODE_FILE {
		return PFSFileWrite(pfsctx, param)
	} else if pfsctx.mode == PFS_MODE_CHUNK {
		return PFSChunkWrite(pfsctx.meta, param)
	}
	return nil, fmt.Errorf("ctx.mode must be `chunk` or `file`")
}

func PFSAllocate(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	return nil, fmt.Errorf("Not implement pfs allocate here")
}

func _PFSFileChdir(path string) error {
	ret, err := C.pfs_chdir(C.CString(path))
	if ret < 0 {
		return err
	}
	return nil
}

func PFSFileChdir(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	_path, ok := param["path"]
	if !ok {
		return nil, errors.New("param.path miss")
	}
	p, ok := _path.(string)
	if !ok {
		return nil, errors.New("param.path must be string")
	}

	p = path.Join(pfsctx.prefix, p)
	return nil, _PFSFileChdir(p)
}

func PFSChdir(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	if pfsctx.mode == PFS_MODE_FILE {
		return PFSFileChdir(pfsctx, param)
	} else if pfsctx.mode == PFS_MODE_CHUNK {
		return PFSChunkChdir(pfsctx.meta, param)
	}
	return nil, fmt.Errorf("ctx.mode must be `chunk` or `file`")
}

func _PFSFileRmdir(path string) error {
	ret, err := C.pfs_rmdir(C.CString(path))
	if ret < 0 {
		return err
	}
	return nil
}

func PFSFileRmdir(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	_path, ok := param["path"]
	if !ok {
		return nil, errors.New("param.path miss")
	}
	p, ok := _path.(string)
	if !ok {
		return nil, errors.New("param.path must be string")
	}

	p = path.Join(pfsctx.prefix, p)
	return nil, _PFSFileRmdir(p)
}

func PFSChunkRmdir(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	return nil, nil
}

func PFSRmdir(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	if pfsctx.mode == PFS_MODE_FILE {
		return PFSFileRmdir(pfsctx, param)
	} else if pfsctx.mode == PFS_MODE_CHUNK {
		return PFSChunkRmdir(pfsctx.meta, param)
	}
	return nil, fmt.Errorf("ctx.mode must be `chunk` or `file`")
}

func _PFSFileReaddir(dirpath, prefix string) ([]string, error) {
	dir, err := C.pfs_opendir(C.CString(dirpath))
	if dir == nil {
		return nil, err
	}

	files := make([]string, 0, 1024)
	for {
		dirent, err := C.pfs_readdir(dir)
		if dirent == nil {
			if err != nil {
				C.pfs_closedir(dir)
				return nil, err
			}
			return files, nil
		}
		// current file is ".", skip it
		if dirent.d_name[0] == '.' && dirent.d_name[1] == 0 {
			continue
		}
		// current file is "..", skip it
		if dirent.d_name[0] == '.' && dirent.d_name[1] == '.' && dirent.d_name[2] == 0 {
			continue
		}

		fn := C.GoString(&dirent.d_name[0])
		filename := path.Join(dirpath, fn)
		filename = strings.Replace(filename, prefix, "", 1)
		filename = strings.TrimLeft(filename, "/")
		files = append(files, filename)
	}

	ret, err := C.pfs_closedir(dir)
	if ret < 0 {
		return nil, err
	}
	return files, nil
}

func _PFSFileReaddirRecursion(dirpath, prefix string) ([]string, error) {
	current := make([]string, 0, 1024)
	dir, err := C.pfs_opendir(C.CString(dirpath))
	if dir == nil {
		return nil, err
	}

	for {
		dirent, err := C.pfs_readdir(dir)
		if dirent == nil {
			if err != nil {
				C.pfs_closedir(dir)
				return nil, err
			}
			break
		}
		// current file is ".", skip it
		if dirent.d_name[0] == '.' && dirent.d_name[1] == 0 {
			continue
		}
		// current file is "..", skip it
		if dirent.d_name[0] == '.' && dirent.d_name[1] == '.' && dirent.d_name[2] == 0 {
			continue
		}

		fn := C.GoString(&dirent.d_name[0])
		filename := path.Join(dirpath, fn)

		isdir, err := _PFSIsDir(filename)
		if err != nil {
			return nil, err
		}
		if isdir {
			sub, err := _PFSFileReaddirRecursion(filename, prefix)
			if err != nil {
				return nil, err
			}
			relativename := strings.Replace(filename, prefix, "", 1)
			name := strings.TrimLeft(relativename, "/")
			current = append(current, name)
			current = append(current, sub...)
		} else {
			relativename := strings.Replace(filename, prefix, "", 1)
			name := strings.TrimLeft(relativename, "/")
			current = append(current, name)
		}
	}

	ret, err := C.pfs_closedir(dir)
	if ret < 0 {
		return nil, err
	}

	return current, nil
}

func PFSFileReaddir_r(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	_path, ok := param["path"]
	if !ok {
		return nil, errors.New("param.path miss")
	}
	p, ok := _path.(string)
	if !ok {
		return nil, errors.New("param.path must be string")
	}

	p = path.Join(pfsctx.prefix, p)
	files, err := _PFSFileReaddirRecursion(p, pfsctx.prefix)
	if err != nil {
		return nil, err
	}
	return files, nil
}

func PFSFileReaddir(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	_path, ok := param["path"]
	if !ok {
		return nil, errors.New("param.path miss")
	}
	p, ok := _path.(string)
	if !ok {
		return nil, errors.New("param.path must be string")
	}

	p = path.Join(pfsctx.prefix, p)
	return _PFSFileReaddir(p, pfsctx.prefix)
}

func PFSUnlink(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}
	_path, ok := param["name"]
	if !ok {
		return nil, errors.New("param.name miss")
	}
	p, ok := _path.(string)
	if !ok {
		return nil, errors.New("param.name must be string")
	}

	if pfsctx.mode == PFS_MODE_FILE {
		p = path.Join(pfsctx.prefix, p)
		ret, err := C.pfs_unlink(C.CString(p))
		if ret <= 0 {
			return nil, err
		}
	} else if pfsctx.mode == PFS_MODE_CHUNK {
		return nil, nil
	}
	return nil, fmt.Errorf("ctx.mode must be `chunk` or `file`")
}

func PFSReaddir_r(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	if pfsctx.mode == PFS_MODE_FILE {
		return PFSFileReaddir_r(pfsctx, param)
	} else if pfsctx.mode == PFS_MODE_CHUNK {
		return PFSChunkReaddir(pfsctx.meta, param)
	}
	return nil, fmt.Errorf("ctx.mode must be `chunk` or `file`")
}

func PFSReaddir(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsctx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}

	if pfsctx.mode == PFS_MODE_FILE {
		return PFSFileReaddir(pfsctx, param)
	} else if pfsctx.mode == PFS_MODE_CHUNK {
		return PFSChunkReaddir(pfsctx.meta, param)
	}
	return nil, fmt.Errorf("ctx.mode must be `chunk` or `file`")
}

func _PFSChunkMetaInit(cluster, pbd string, mode uint) (*PFSChunkMeta, error) {
	var m PFSChunkMeta
	m.meta = C.pfs_chunkstream_init(C.CString(cluster), C.CString(pbd), C.int(mode))
	if m.meta == nil {
		return nil, errors.New("metainit invalid pbd")
	}
	return &m, nil
}

func _PFSChunkMetaFini(meta *PFSChunkMeta) {
	C.pfs_chunkstream_fini(meta.meta)
}

func _PFSChunkFileCount(meta *PFSChunkMeta) int {
	var count C.int
	C.pfs_chunkstream_get_nchunk(meta.meta, &count)
	return int(count)
}

func _PFSChunkFileOpen(meta *PFSChunkMeta, ckid int) (*PFSChunkStream, error) {
	var stream PFSChunkStream
	stream.file = C.pfs_chunkstream_open(meta.meta, C.int(ckid))
	if stream.file == nil {
		return nil, errors.New("open chunk fail")
	}
	return &stream, nil
}

func _PFSChunkFileClose(stream *PFSChunkStream) {
	C.pfs_chunkstream_close(stream.file)
}

func _PFSChunkFileRead(file *PFSChunkStream, buf []byte) (int64, error) {
	c_buf := (*C.char)(unsafe.Pointer(&buf[0]))
	c_buf_len := C.ulong(len(buf))
	size := int64(C.pfs_chunkstream_read(file.file, c_buf, c_buf_len))
	if size < 0 {
		return 0, errors.New("pfs read fail")
	} else if size == 0 {
		return 0, io.EOF
	} else {
		return size, nil
	}
}

func (file *PFSChunkStream) Read(buf []byte) (int, error) {
	size, err := _PFSChunkFileRead(file, buf)
	return int(size), err
}

func _PFSChunkFileWrite(file *PFSChunkStream, buf []byte) (int64, error) {
	c_buf := (*C.char)(unsafe.Pointer(&buf[0]))
	c_buf_len := C.ulong(len(buf))
	size := int64(C.pfs_chunkstream_write(file.file, c_buf, c_buf_len))
	if size <= 0 {
		return 0, errors.New("pfs write fail")
	} else {
		return size, nil
	}
}

func (file *PFSChunkStream) Write(buf []byte) (int, error) {
	size, err := _PFSChunkFileWrite(file, buf)
	return int(size), err
}

// fini(ctx, param) -> nil, error
// param -- unused
func PFSChunkFini(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	meta, ok := ctx.(*PFSChunkMeta)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}
	_PFSChunkMetaFini(meta)
	return nil, nil
}

// create(ctx, param) -> *PFSChunkStream, error
// param["name"] -- string
func PFSChunkCreate(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	return PFSChunkOpen(ctx, param)
}

// open(ctx, param) -> *PFSChunkStream, error
// param["name"] -- string
func PFSChunkOpen(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	meta, ok := ctx.(*PFSChunkMeta)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}
	_chunkIDStr, ok := param["name"]
	if !ok {
		return nil, errors.New("param.name miss")
	}
	chunkIDStr, ok := _chunkIDStr.(string)
	if !ok {
		return nil, errors.New("param.name must be string")
	}

	chunkID, err := strconv.ParseInt(chunkIDStr, 10, 32)
	if err != nil {
		return nil, errors.New("param.name must be digtal string")
	}
	return _PFSChunkFileOpen(meta, int(chunkID))
}

// close(ctx, param) -> *PFSChunkStream, error
// param["file"] -- *PFSChunkStream
func PFSChunkClose(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}
	_stream, ok := param["file"]
	if !ok {
		return nil, errors.New("param.file miss")
	}
	stream, ok := _stream.(*PFSChunkStream)
	if !ok {
		return nil, errors.New("param.file must be *PFSChunkStream")
	}
	_PFSChunkFileClose(stream)
	return nil, nil
}

// read(ctx, param) -> int64, error
// param["file"] -- *PFSChunkStream
// param["buf"]  -- []byte
func PFSChunkRead(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}
	_stream, ok := param["file"]
	if !ok {
		return nil, errors.New("param.file miss")
	}
	stream, ok := _stream.(*PFSChunkStream)
	if !ok {
		return nil, errors.New("param.file must be *PFSChunkStream")
	}
	_buf, ok := param["buf"]
	if !ok {
		return nil, errors.New("param.buf miss")
	}
	buf, ok := _buf.([]byte)
	if !ok {
		return nil, errors.New("param.buf must be []byte")
	}
	return _PFSChunkFileRead(stream, buf)
}

// write(ctx, param) -> int64, error
// param["file"] -- *PFSChunkStream
// param["buf"]  -- []byte
func PFSChunkWrite(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PFSCtx")
	}
	_stream, ok := param["file"]
	if !ok {
		return nil, errors.New("param.file miss")
	}
	stream, ok := _stream.(*PFSChunkStream)
	if !ok {
		return nil, errors.New("param.file must be *PFSChunkStream")
	}
	_buf, ok := param["buf"]
	if !ok {
		return nil, errors.New("param.buf miss")
	}
	buf, ok := _buf.([]byte)
	if !ok {
		return nil, errors.New("param.buf must be []byte")
	}
	return _PFSChunkFileWrite(stream, buf)
}

// chdir(ctx, param) -> nil, error
// param  -- unsued
func PFSChunkChdir(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	return nil, nil
}

// readdir(ctx, param) -> []string(filenames), error
// param  -- unsued
func PFSChunkReaddir(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	meta, ok := ctx.(*PFSChunkMeta)
	if !ok {
		return nil, errors.New("ctx must be *PfsChunkMeta")
	}
	count := _PFSChunkFileCount(meta)
	fns := make([]string, count)

	for i := 0; i < count; i++ {
		fns[i] = strconv.FormatInt(int64(i), 10)
	}
	return fns, nil
}

func PFSChunkIsDir(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	return false, nil
}

func PFSGetPath(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	pfsCtx, ok := ctx.(*PFSCtx)
	if !ok {
		return nil, errors.New("ctx must be *PfsCtx")
	}

	return pfsCtx.prefix, nil
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
	funcs := make(map[string]func(ctx interface{}, param map[string]interface{}) (interface{}, error))
	funcs["init"] = PFSInit
	funcs["fini"] = PFSFini
	funcs["mkdirs"] = PFSMkdirs
	funcs["open"] = PFSOpen
	funcs["create"] = PFSCreate
	funcs["fallocate"] = PFSFallocate
	funcs["unlink"] = PFSUnlink
	funcs["close"] = PFSClose
	funcs["read"] = PFSRead
	funcs["write"] = PFSWrite
	funcs["rmdir"] = PFSRmdir
	funcs["chdir"] = PFSChdir
	funcs["readdir"] = PFSReaddir
	funcs["readdir_r"] = PFSReaddir_r
	funcs["isdir"] = PFSIsDir
	funcs["size"] = PFSSize
	funcs["rename"] = PFSRename
	funcs["access"] = PFSAccess
	funcs["getpath"] = PFSGetPath
	funcs["exist"] = PFSExist
	funcs["du"] = PFSDu
	funcs["seek"] = PFSSeek
	funcs["truncate"] = PFSTruncate
	return funcs, nil
}

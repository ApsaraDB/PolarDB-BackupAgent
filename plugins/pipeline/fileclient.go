package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
)

type HTTPCtx struct {
	endpoint string
	instance string
	path     string
}

const (
	HTTP_MODE_READ  = 1
	HTTP_MODE_WRITE = 2
)

type HTTPFile struct {
	name   string
	mode   int
	reader io.ReadCloser
	ri     *ResData
}

type ResData struct {
	Code  int      `json:"code"`
	Error string   `json:"error"`
	Size  int      `json:"size"`
	List  []string `json:"list"`
}

func (file *HTTPFile) Read(buf []byte) (int, error) {
	if file.mode != HTTP_MODE_READ {
		return 0, errors.New("only support read in download mode")
	}
	return file.reader.Read(buf)
}

func (file *HTTPFile) Write(buf []byte) (int, error) {
	if file.mode != HTTP_MODE_WRITE {
		return 0, errors.New("only support write in upload mode")
	}
	return 0, nil
}

func HTTPReaddir(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	httpctx, ok := ctx.(*HTTPCtx)
	if !ok {
		return nil, errors.New("ctx must be *HTTPCtx")
	}

	relPath := ""
	_path, ok := param["path"]
	if ok {
		relPath, ok = _path.(string)
		if !ok {
			return nil, errors.New("param.path must be string")
		}
	}

	var nullbyte []byte
	r := bytes.NewReader(nullbyte)
	URL := "http://" + httpctx.endpoint
	u, err := url.Parse(URL)
	if err != nil {
		return nil, err
	}

	u.Path = path.Join(u.Path, "readdir")

	resp, err := http.Post(u.String()+"?dir="+httpctx.path+relPath, "application/json", r)
	if err != nil {
		return nil, errors.New("HTTPReaddir failed: " + err.Error())
	}
	defer resp.Body.Close()

	respContent, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.New("HTTPReaddir read response failed: " + err.Error())
	}

	var respInfo ResData
	err = json.Unmarshal(respContent, &respInfo)
	if err != nil {
		return nil, errors.New("HTTPReaddir Unmarshal response data " + string(respContent) + " failed: " + err.Error())
	}
	if respInfo.Code != 0 {
		return nil, errors.New("HTTPReaddir execute failed: " + respInfo.Error)
	}

	return respInfo.List, nil
}

func HTTPOpen(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	HTTP, ok := ctx.(*HTTPCtx)
	if !ok {
		return nil, errors.New("ctx must be *HTTPCtx")
	}
	_filepath, ok := param["name"]
	if !ok {
		return nil, errors.New("param.name miss")
	}

	filepath, ok := _filepath.(string)
	if !ok {
		return nil, errors.New("param.name msut be string")
	}

	resp, err := http.Get("http://" + HTTP.endpoint + "/download/" + HTTP.path + "/" + filepath)
	if err != nil {
		return nil, errors.New("HTTPOpen failed:" + err.Error())
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("HTTPOpen failed:" + resp.Status)
	}

	file := &HTTPFile{
		mode:   HTTP_MODE_READ,
		name:   filepath,
		reader: resp.Body,
	}
	return file, nil
}

func HTTPClose(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*HTTPCtx)
	if !ok {
		return nil, errors.New("ctx must be *HTTPCtx")
	}
	_file, ok := param["file"]
	if !ok {
		return nil, errors.New("param.file miss")
	}
	file, ok := _file.(*HTTPFile)
	if !ok {
		return nil, errors.New("param.file msut be *HTTPFile")
	}
	if file.mode == HTTP_MODE_WRITE {
		return nil, nil
	} else if file.mode == HTTP_MODE_READ {
		err := file.reader.Close()
		if err != nil {
			return nil, err
		}
		return nil, nil
	}
	return nil, errors.New("invalid file mode")
}
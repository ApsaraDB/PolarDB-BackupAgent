package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"path"
	"strings"
	"time"
)

var (
	filePath string
	addr     string
)

type HTTPConf struct {
	Endpoint   string `json:"Endpoint"`
	InstanceID string `json:"InstanceID"`
	BackupID   string `json:"BackupID"`
}

type HTTPCtx struct {
	endpoint string
	instance string
	backupid string
	path     string
}

const (
	HTTP_MODE_READ  = 1
	HTTP_MODE_WRITE = 2
)

type HTTPFile struct {
	name   string
	mode   int
	w      io.Writer
	pw     *io.PipeWriter
	bw     *multipart.Writer
	reader io.ReadCloser
	ri     *ResponseData
}

type ResponseData struct {
	Code  int      `json:"code"`
	Error string   `json:"error"`
	Size  int      `json:"size"`
	List  []string `json:"list"`
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
	return file.w.Write(buf)
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

	var respInfo ResponseData
	err = json.Unmarshal(respContent, &respInfo)
	if err != nil {
		return nil, errors.New("HTTPReaddir Unmarshal response data " + string(respContent) + " failed: " + err.Error())
	}
	if respInfo.Code != 0 {
		return nil, errors.New("HTTPReaddir execute failed: " + respInfo.Error)
	}

	return respInfo.List, nil
}

func HTTPUnlink(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	httpctx, ok := ctx.(*HTTPCtx)
	if !ok {
		return nil, errors.New("ctx must be *HTTPCtx")
	}
	_name, ok := param["name"]
	if !ok {
		return nil, errors.New("param.name miss")
	}
	name, ok := _name.(string)
	if !ok {
		return nil, errors.New("param.name msut be string")
	}

	var nullbyte []byte
	r := bytes.NewReader(nullbyte)
	URL := "http://" + httpctx.endpoint
	u, err := url.Parse(URL)
	if err != nil {
		return nil, err
	}

	u.Path = path.Join(u.Path, "unlink")

	resp, err := http.Post(u.String()+"?file="+httpctx.path+"/"+name, "application/json", r)
	if err != nil {
		return nil, errors.New("HTTPUnlink failed: " + err.Error())
	}
	defer resp.Body.Close()

	respContent, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.New("HTTPUnlink read response failed: " + err.Error())
	}

	var respInfo ResponseData
	err = json.Unmarshal(respContent, &respInfo)
	if err != nil {
		return nil, errors.New("HTTPUnlink Unmarshal response data " + string(respContent) + " failed: " + err.Error())
	}
	if respInfo.Code != 0 {
		return nil, errors.New("HTTPUnlink execute failed: " + respInfo.Error)
	}

	return nil, nil
}

func HTTPExist(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	httpctx, ok := ctx.(*HTTPCtx)
	if !ok {
		return nil, errors.New("ctx must be *HTTPCtx")
	}
	_name, ok := param["path"]
	if !ok {
		return nil, errors.New("param.path miss")
	}
	name, ok := _name.(string)
	if !ok {
		return nil, errors.New("param.path msut be string")
	}

	var nullbyte []byte
	r := bytes.NewReader(nullbyte)
	URL := "http://" + httpctx.endpoint
	u, err := url.Parse(URL)
	if err != nil {
		return nil, err
	}

	u.Path = path.Join(u.Path, "exist")

	resp, err := http.Post(u.String()+"?file="+httpctx.path+"/"+name, "application/json", r)
	if err != nil {
		return nil, errors.New("HTTPExist failed: " + err.Error())
	}
	defer resp.Body.Close()

	respContent, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.New("HTTPExist read response failed: " + err.Error())
	}

	var respInfo ResponseData
	err = json.Unmarshal(respContent, &respInfo)
	if err != nil {
		return nil, errors.New("HTTPExist Unmarshal response data " + string(respContent) + " failed: " + err.Error())
	}
	if respInfo.Code == 0 {
		return true, nil
	} else if respInfo.Code == 1 {
		return false, nil
	} else {
		return false, errors.New("HTTPExist execute failed: " + respInfo.Error)
	}

	return false, nil
}

func HTTPSize(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	httpctx, ok := ctx.(*HTTPCtx)
	if !ok {
		return nil, errors.New("ctx must be *HTTPCtx")
	}
	_name, ok := param["name"]
	if !ok {
		return nil, errors.New("param.path miss")
	}
	name, ok := _name.(string)
	if !ok {
		return nil, errors.New("param.path msut be string")
	}

	var nullbyte []byte
	r := bytes.NewReader(nullbyte)
	URL := "http://" + httpctx.endpoint
	u, err := url.Parse(URL)
	if err != nil {
		return nil, err
	}

	u.Path = path.Join(u.Path, "size")

	resp, err := http.Post(u.String()+"?file="+httpctx.path+"/"+name, "application/json", r)
	if err != nil {
		return nil, errors.New("HTTPSize failed: " + err.Error())
	}
	defer resp.Body.Close()

	respContent, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.New("HTTPSize read response failed: " + err.Error())
	}

	var respInfo ResponseData
	err = json.Unmarshal(respContent, &respInfo)
	if err != nil {
		return nil, errors.New("HTTPSize Unmarshal response data: [" + string(respContent) + "] failed: " + err.Error())
	}
	if respInfo.Code != 0 {
		return 0, errors.New("HTTPSize execute failed: " + respInfo.Error)
	}

	return respInfo.Size, nil
}

func HTTPRemoveAll(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	httpctx, ok := ctx.(*HTTPCtx)
	if !ok {
		return nil, errors.New("ctx must be *HTTPCtx")
	}
	_name, ok := param["path"]
	if !ok {
		return nil, errors.New("param.path miss")
	}
	name, ok := _name.(string)
	if !ok {
		return nil, errors.New("param.path msut be string")
	}

	var nullbyte []byte
	r := bytes.NewReader(nullbyte)
	URL := "http://" + httpctx.endpoint
	u, err := url.Parse(URL)
	if err != nil {
		return nil, err
	}

	u.Path = path.Join(u.Path, "rmdir")

	resp, err := http.Post(u.String()+"?file="+httpctx.path+"/"+name, "application/json", r)
	if err != nil {
		return nil, errors.New("HTTPRemoveAll failed: " + err.Error())
	}
	defer resp.Body.Close()

	respContent, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.New("HTTPRemoveAll read response failed: " + err.Error())
	}

	var respInfo ResponseData
	err = json.Unmarshal(respContent, &respInfo)
	if err != nil {
		return nil, errors.New("HTTPRemoveAll Unmarshal response data " + string(respContent) + " failed: " + err.Error())
	}
	if respInfo.Code == 0 {
		return true, nil
	} else if respInfo.Code == 1 {
		return false, nil
	} else {
		return false, errors.New("HTTPRemoveAll execute failed: " + respInfo.Error)
	}

	return false, nil
}

func HTTPInit(ctx interface{}, param map[string]interface{}) (interface{}, error) {
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

	var initConf HTTPConf
	err := json.Unmarshal(conf, &initConf)
	if err != nil {
		return nil, errors.New("parse plugin conf failed")
	}

	if initConf.Endpoint == "" {
		return nil, errors.New("Endpoint is null, please check the configuration")
	}

	// if initConf.InstanceID == "" || initConf.BackupID == "" {
	// 	return nil, errors.New("InstanceID or BackupID is null, please check the configuration")
	// }

	var nullbyte []byte
	r := bytes.NewReader(nullbyte)
	URL := "http://" + initConf.Endpoint
	u, err := url.Parse(URL)
	if err != nil {
		return nil, err
	}

	u.Path = path.Join(u.Path, "mkdir")

	resp, err := http.Post(u.String()+"?file="+initConf.InstanceID+"/"+initConf.BackupID, "application/json", r)
	if err != nil {
		return nil, errors.New("HTTPInit post mkdir " + initConf.InstanceID + "/" + initConf.BackupID + " failed: " + err.Error())
	}
	defer resp.Body.Close()

	respContent, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.New("HTTPInit read response failed: " + err.Error())
	}

	var respInfo ResponseData
	err = json.Unmarshal(respContent, &respInfo)
	if err != nil {
		return nil, errors.New("HTTPInit Unmarshal response data " + string(respContent) + " failed: " + err.Error())
	}
	if respInfo.Code != 0 {
		return nil, errors.New("HTTPInit execute mkdir failed: " + respInfo.Error)
	}

	HTTPctx := &HTTPCtx{
		endpoint: initConf.Endpoint,
		instance: initConf.InstanceID,
		backupid: initConf.BackupID,
		path:     initConf.InstanceID + "/" + initConf.BackupID,
	}

	return HTTPctx, nil
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

var quoteEscaper = strings.NewReplacer("\\", "\\\\", `"`, "\\\"")

func escapeQuotes(s string) string {
	return quoteEscaper.Replace(s)
}

func HTTPCreate(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	HTTP, ok := ctx.(*HTTPCtx)
	if !ok {
		return nil, errors.New("ctx must be *HTTPCtx")
	}
	_filePath, ok := param["name"]
	if !ok {
		return nil, errors.New("param.name miss")
	}

	filePath, ok := _filePath.(string)
	if !ok {
		return nil, errors.New("param.name msut be string")
	}

	pr, pw := io.Pipe()

	var respInfo ResponseData
	respInfo.Code = -1
	bw := multipart.NewWriter(pw)
	go func() {
		url := fmt.Sprintf("http://%s/upload", HTTP.endpoint)
		req, err := http.NewRequest("POST", url, pr)

		//add headers
		req.Header.Add("Content-Type", bw.FormDataContentType())

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			client.CloseIdleConnections()
			return
		}
		respContent, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			respInfo.Code = 1
			respInfo.Error = err.Error()
			client.CloseIdleConnections()
			return
		}
		err = json.Unmarshal(respContent, &respInfo)
		if err != nil {
			respInfo.Code = 1
			respInfo.Error = err.Error()
			client.CloseIdleConnections()
			return
		}
		resp.Body.Close()

		client.CloseIdleConnections()
	}()

	// text part1
	p1w, _ := bw.CreateFormField("name")
	p1w.Write([]byte("backup"))

	// text part2
	p2w, _ := bw.CreateFormField("type")
	p2w.Write([]byte("http"))

	// file part1
	fileName := HTTP.path + "/" + filePath
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition",
		fmt.Sprintf(`form-data; name="%s"; filename="%s"`,
			escapeQuotes("file1"), escapeQuotes(fileName)))
	h.Set("Content-Type", "application/pdf")
	fw1, _ := bw.CreatePart(h)

	file := &HTTPFile{
		mode: HTTP_MODE_WRITE,
		name: fileName,
		w:    fw1,
		bw:   bw,
		pw:   pw,
		ri:   &respInfo,
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
		err := file.bw.Close()
		if err != nil {
			return nil, err
		}
		err = file.pw.Close()
		if err != nil {
			return nil, err
		}

		for i := 0; i < 1000; i++ {
			if file.ri.Code != -1 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if file.ri.Code == -1 {
			return nil, errors.New("wait too long for response when close")
		}

		if file.ri.Code != 0 {
			return nil, errors.New(file.ri.Error)
		}

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

func HTTPGetPath(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	httpctx, ok := ctx.(*HTTPCtx)
	if !ok {
		return nil, errors.New("ctx must be *HTTPCtx")
	}

	return httpctx.endpoint, nil
}

func HTTPWrite(ctx interface{}, param map[string]interface{}) (interface{}, error) {
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

func HTTPRead(ctx interface{}, param map[string]interface{}) (interface{}, error) {
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

func HTTPFini(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*HTTPCtx)
	if !ok {
		return nil, errors.New("ctx must be *HTTPCtx")
	}
	return nil, nil
}

func HTTPEmpty(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	_, ok := ctx.(*HTTPCtx)
	if !ok {
		return nil, errors.New("ctx must be *HTTPCtx")
	}
	return nil, nil
}

func ExportMapping(ctx interface{}, param map[string]interface{}) (interface{}, error) {
	funcs := make(map[string]func(interface{}, map[string]interface{}) (interface{}, error))
	funcs["init"] = HTTPInit
	funcs["fini"] = HTTPFini
	funcs["mkdirs"] = HTTPEmpty
	funcs["unlink"] = HTTPUnlink
	funcs["open"] = HTTPOpen
	funcs["create"] = HTTPCreate
	funcs["close"] = HTTPClose
	funcs["read"] = HTTPRead
	funcs["write"] = HTTPWrite
	funcs["chdir"] = HTTPEmpty
	funcs["readdir"] = HTTPReaddir
	funcs["readdir_r"] = HTTPReaddir
	funcs["isdir"] = HTTPEmpty
	funcs["rmdir"] = HTTPRemoveAll
	funcs["size"] = HTTPSize
	funcs["fallocate"] = HTTPEmpty
	funcs["getpath"] = HTTPEmpty
	funcs["exist"] = HTTPExist
	return funcs, nil
}

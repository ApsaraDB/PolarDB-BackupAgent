package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"runtime"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	storePath string
	logPath   string
	addr      string
)

var httpflogger *log.Logger

const (
	DEFAULT_LOG_PATH = "/tmp"
)

func CopyBuffer(dst io.Writer, src io.Reader, buf []byte) (written int64, crc uint32, err error) {
	if buf == nil {
		return 0, 0, errors.New("buf cannot be nil")
	}
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			crc = crc32.Update(crc, crc32.IEEETable, buf[:nr])
			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return written, crc, err
}

func handleUploadFile(w http.ResponseWriter, r *http.Request) {
	t1 := time.Now()

	if r.Form == nil {
		err := r.ParseForm()
		if err != nil {
			httpflogger.Printf("[ERROR] ParseForm failed: %s", err.Error())
			return
		}
	}
	if r.MultipartForm != nil {
		return
	}

	v := r.Header.Get("Content-Type")
	if v == "" {
		httpflogger.Printf("[ERROR] get header failed: ErrNotMultipart")
		return
	}
	allowMixed := false
	d, params, err := mime.ParseMediaType(v)
	if err != nil || !(d == "multipart/form-data" || allowMixed && d == "multipart/mixed") {
		httpflogger.Printf("[ERROR] ParseMediaType failed: ErrNotMultipart")
		return
	}
	boundary, ok := params["boundary"]
	if !ok {
		httpflogger.Printf("[ERROR] ErrMissingBoundary")
		return
	}
	mr := multipart.NewReader(r.Body, boundary)

	for {
		p, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			httpflogger.Printf("[ERROR] read NextPart failed: %s", err.Error())
			return
		}

		name := p.FormName()
		if name == "" {
			continue
		}
		filename := p.FileName()

		if filename == "" {
			continue
		}

		localFileName := storePath + "/" + filename
		out, err := os.Create(localFileName)
		if err != nil {
			httpflogger.Printf("[ERROR] failed to open the file %s for writing", localFileName)
			err := fmt.Errorf(`{"error": "%s", "code": 1002}`, err.Error())
			res := err.Error()
			fmt.Fprintf(w, "%s", res)
			return
		}

		var buf = make([]byte, 1024*128)
		_, _, err = CopyBuffer(out, p, buf)
		if err != nil && err != io.EOF {
			httpflogger.Printf("[ERROR] copy buffer to file %s err:%s\n", localFileName, err.Error())
			out.Close()
			err := fmt.Errorf(`{"error": "%s", "code": 1002}`, err.Error())
			res := err.Error()
			fmt.Fprintf(w, "%s", res)
			return
		}

		out.Sync()
		out.Close()
		res := `{"error": "", "code": 0}`
		fmt.Fprintf(w, "%s", res)

		httpflogger.Printf("[INFO] file %s uploaded ok\n", filename)
	}

	httpflogger.Printf("[INFO] time consume %s\n", time.Now().Sub(t1))
}

func handleUnlinkFile(w http.ResponseWriter, r *http.Request) {
	var res string
	file := r.URL.Query().Get("file")
	err := os.Remove(storePath + "/" + file)
	if err != nil {
		err := fmt.Errorf(`{"error": "%s", "code": 1002}`, err.Error())
		res = err.Error()
	} else {
		res = `{"error": "", "code": 0}`
	}

	fmt.Fprintf(w, "%s", res)
}

func handleExistFile(w http.ResponseWriter, r *http.Request) {
	var res string
	file := r.URL.Query().Get("file")
	_, err := os.Stat(storePath + "/" + file)
	if err != nil {
		if os.IsNotExist(err) {
			err = fmt.Errorf(`{"error": "", "code": 1}`)
		} else {
			err = fmt.Errorf(`{"error": "%s", "code": 1002}`, err.Error())
		}
		res = err.Error()
	} else {
		res = `{"error": "", "code": 0}`
	}

	fmt.Fprintf(w, "%s", res)
}

func handleSizeFile(w http.ResponseWriter, r *http.Request) {
	var res string
	file := r.URL.Query().Get("file")

	fi, err := os.Stat(storePath + "/" + file)
	if err != nil {
		err = fmt.Errorf(`{"error": "%s", "code": 1002}`, err.Error())
		res = err.Error()
	} else {
		res = fmt.Sprintf(`{"error": "", "code": 0, "size": %d}`, fi.Size())
	}

	httpflogger.Printf("[INFO] !! handle size file ret: %s\n", res)

	fmt.Fprintf(w, "%s", res)
}

func handleMkdir(w http.ResponseWriter, r *http.Request) {
	var res string
	file := r.URL.Query().Get("file")

	err := os.MkdirAll(storePath+"/"+file, 0700)
	if err != nil {
		err = fmt.Errorf(`{"error": "%s", "code": 1002}`, err.Error())
		res = err.Error()
	} else {
		res = `{"error": "", "code": 0}`
	}

	fmt.Fprintf(w, "%s", res)
}

func handleRmdir(w http.ResponseWriter, r *http.Request) {
	var res string
	file := r.URL.Query().Get("file")

	err := os.RemoveAll(storePath + "/" + file)
	if err != nil {
		err = fmt.Errorf(`{"error": "%s", "code": 1002}`, err.Error())
		res = err.Error()
	} else {
		res = `{"error": "", "code": 0}`
	}

	fmt.Fprintf(w, "%s", res)
}

func handleReaddir(w http.ResponseWriter, r *http.Request) {
	var res string

	dir := r.URL.Query().Get("dir")

	file, err := os.Open(storePath + "/" + dir)
	if err != nil {
		res = fmt.Sprintf(`{"error": "%s", "code": 1002}`, err.Error())
		fmt.Fprintf(w, "%s", res)
		return
	}

	files, err := file.Readdirnames(-1)
	if err != nil {
		res = fmt.Sprintf(`{"error": "%s", "code": 1002}`, err.Error())
		fmt.Fprintf(w, "%s", res)
		return
	}

	filesJson, _ := json.Marshal(files)
	res = fmt.Sprintf(`{"error": "", "code": 0, "list": %s}`, string(filesJson))

	fmt.Fprintf(w, "%s", res)
}

func init() {
	flag.StringVar(&storePath, "mount", "", "the dir to store files")
	flag.StringVar(&addr, "addr", ":8080", "the addr of file server")
	flag.StringVar(&logPath, "log", "/tmp", "the dir to store log of server")
	flag.Parse()
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func PrintMemUsagePeriodically() {
	go func() {
		for true {
			PrintMemUsage()
			time.Sleep(time.Duration(1000) * time.Millisecond)
		}
	}()
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func initLog() error {
	if logPath == "" {
		logPath = DEFAULT_LOG_PATH
		fmt.Println("[INFO] dir of log is not specified, set to %s default", DEFAULT_LOG_PATH)
	}

	err := os.MkdirAll(logPath, os.ModePerm)
	if err != nil {
		return err
	}

	compress := false
	fn := path.Join(logPath, "httpfiled.log")
	size := 10
	count := 128
	age := 30

	logger := &lumberjack.Logger{
		Filename:   fn,
		Compress:   compress,
		MaxSize:    size,
		MaxBackups: count,
		MaxAge:     age,
		LocalTime:  true,
	}
	httpflogger = log.New(logger, "", log.LstdFlags|log.Lshortfile)
	httpflogger.Printf("[INFO] main init log success, logpath: %s\n", fn)

	return nil
}

func main() {
	if storePath == "" {
		fmt.Println("[ERROR] Please config dir for storing files first")
		return
	}

	err := initLog()
	if err != nil {
		fmt.Println("[ERROR] Init log failed: %s", err.Error())
		return
	}

	// PrintMemUsagePeriodically()

	http.HandleFunc("/upload", handleUploadFile)

	fs := http.FileServer(http.Dir(storePath))
	http.Handle("/download/", http.StripPrefix("/download", fs))

	http.HandleFunc("/unlink", handleUnlinkFile)

	http.HandleFunc("/exist", handleExistFile)

	http.HandleFunc("/size", handleSizeFile)

	http.HandleFunc("/mkdir", handleMkdir)

	http.HandleFunc("/readdir", handleReaddir)

	http.HandleFunc("/rmdir", handleRmdir)

	httpflogger.Printf("[INFO] http file server start\n")

	http.ListenAndServe(addr, nil)
}

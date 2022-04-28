package main

import (
	"encoding/json"
    "fmt"
    "os"
    "net/http"
)

var (
	blockFilePath string
)

func handleReaddir(w http.ResponseWriter, r *http.Request) {
	var res string

	dir := r.URL.Query().Get("dir")

	file, err := os.Open(blockFilePath + "/" + dir)
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

func (srv *Server) startBlockFileServer() error {
	mux := http.NewServeMux()

	mux.HandleFunc("/readdir", handleReaddir)

	blockFilePath = srv.conf.Ctrl.BlockBackupDir + "/polar_block_backup"

	_, err := os.Stat(blockFilePath)
	if os.IsNotExist(err) {
		err = os.MkdirAll(blockFilePath, 0700)
	}
	if err != nil {
		return err
	}

    fs := http.FileServer(http.Dir(blockFilePath))
	mux.Handle("/download/", http.StripPrefix("/download", fs))

    bfsrv := &http.Server{Addr: srv.conf.Ctrl.BlockServerPort, Handler: mux}

    go func() {
        if err := bfsrv.ListenAndServe(); err != http.ErrServerClosed {
            srv.logger.Printf("[ERROR] block file server listen and serve failed: %s\n", err.Error())
        }
    }()

	return nil
}

// func stopBlockFileServer(ctx *BackupCtx) error {
// 	if ctx.blockFileServer != nil {
// 		if err := ctx.blockFileServer.Shutdown(context.Background()); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }
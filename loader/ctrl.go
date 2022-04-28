package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"path"
	"reflect"
	"sync"
)

type Ctrl struct {
	logger   *log.Logger
	addr     string
	notify   chan []byte
	srv      *Server
	http     *http.Server
	callback sync.Map // map[string]func(interface{}, map[string]interface{})(interface{},error)
}

// a directory with a plugin.conf (PluginConf format)
type NewPlugin struct {
	ZipPath string `json:"ZipPath"`
}

func (ctrl *Ctrl) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method == "GET" && r.URL.Path == "/Keepalive" {
		fmt.Fprintf(w, `{"error": null, "code": 0}`)
		return
	}

	if r.Method != "POST" {
		return
	}

	if r.Method == "POST" && r.URL.Path == "/NewPlugin" {
		_, _ = ioutil.ReadAll(r.Body)
		return
	}

	_callback, ok := ctrl.callback.Load(r.URL.Path)
	if !ok {
		fmt.Fprintf(w, `{"error": "invalid request path: %s", "code": 1}`, r.URL.Path)
		return
	}

	pluginName := path.Dir(r.URL.Path)[1:]
	_plugin, _ := ctrl.srv.plugins.Load(pluginName)
	plugin := _plugin.(*PluginInfo)

	conf, _ := ctrl.srv.pluginConfs.Load(pluginName)
	funcname := conf.(PluginConf).Ctrl[path.Base(r.URL.Path)]

	// ctrl.logger.Printf("[INFO] ctrl new http request [%s], hander [%s.%s]\n", r.URL.Path, pluginName, funcname)
	callback := _callback.(func(interface{}, map[string]interface{}) (interface{}, error))

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return
	}

	param := make(map[string]interface{})
	param["req"] = data
	param["query"] = r.URL.Query()
	param["notify"] = ctrl.srv.notify
	param["imports"] = &ctrl.srv.exports
	status, err := callback(plugin.ctx, param)
	if err != nil {
		ctrl.logger.Printf("[ERROR] ctrl new http request [%s], hander [%s.%s], error: %s\n", r.URL.Path, pluginName, funcname, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
	}
	fmt.Fprintf(w, "%s", status.(string))
}

func (ctrl *Ctrl) InitCallback() {
	ctrl.logger.Printf("[INFO] ctrl add callbacks\n")
	ctrl.srv.plugins.Range(func(k, v interface{}) bool {
		name := k.(string)
		plugin := v.(*PluginInfo)
		for subPath, _fun := range plugin.ctrl {
			conf, _ := ctrl.srv.pluginConfs.Load(name)
			funcname := conf.(PluginConf).Ctrl[subPath]

			fun, ok := _fun.(func(interface{}, map[string]interface{}) (interface{}, error))
			if !ok {
				ctrl.logger.Printf("[ERROR] ctrl [%s/%s] callback [%s.%s] must be func(interface{}, map[string]interface{})(interface{}, error), current is: %s, %p\n", name, subPath, name, funcname, reflect.TypeOf(fun), fun)
				continue
			}
			ctrl.logger.Printf("[INFO] ctrl add callback [%s.%s] ==> [%s/%s] success\n", name, funcname, name, subPath)
			ctrl.callback.Store(path.Join("/", name, subPath), fun)
		}
		return true
	})
}

func (ctrl *Ctrl) Init(srv *Server) {
	ctrl.srv = srv
	ctrl.logger = srv.logger
	ctrl.notify = srv.notify
	ctrl.addr = srv.conf.Ctrl.ListenAddr
	ctrl.callback = sync.Map{}
	ctrl.InitCallback()
}

func (ctrl *Ctrl) Exit() {
}

func (ctrl *Ctrl) Start() {
	ctrl.http = &http.Server{
		Addr:    ctrl.addr,
		Handler: ctrl,
	}
	go func() {
		ctrl.http.ListenAndServe()
	}()
}

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"gopkg.in/natefinch/lumberjack.v2"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"syscall"

	pb "backup/loader/cmctrl/polardb"
	"net/http"
	_ "net/http/pprof"
)

type PluginInfo struct {
	deps   map[string]*PluginInfo
	module *ModuleInfo
	ctx    interface{}
	ctrl   map[string]interface{}
	mode   string
	conf   []byte
	isDll  bool
}

type PluginInstance struct {
	plug *PluginInfo
	stop chan bool
	ctx  interface{}
	id   string
}

type Server struct {
	binPath  string
	confPath string
	confFlag map[string]*string // use for comdline input, no dynamic load

	conf    *GlobalConf
	plugins sync.Map // map[pluginName]*PluginInfo
	modules sync.Map // map[modulePath]*ModuleInfo
	// confrim all the plugins conf are loaded before load module
	pluginConfs sync.Map // map[pluginName]pluginConf
	ctrl        Ctrl
	notify      chan []byte
	exports     sync.Map // map[pluginName][module.exports(funcs)]

	daemon bool
	entry  string
	stop   bool

	// map[jobid][pluginctx]
	instance   string   // only one instance use in command mode
	instances  sync.Map // multi instances in daemon mode
	logger     *log.Logger
	loggerCmd  *log.Logger
	loggerFile *log.Logger

	lockFile string
	pidFile  string

	mutex sync.Mutex
}

const (
    procsFile       = "cgroup.procs"
    memoryLimitFile = "memory.limit_in_bytes"
    cgroupRootPath  = "/sys/fs/cgroup/memory/backup-agent"
)

func NewServer() *Server {
	var srv Server

	errHelp := flag.ErrHelp
	flag.ErrHelp = nil

	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	flag.StringVar(&srv.binPath, "config", "/usr/local/polardb_o_backup_tool_current/bin", "bin path")

	flag.Parse()
	srv.confPath = srv.binPath + "/loader.conf"
	flag.ErrHelp = errHelp
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	srv.confFlag = make(map[string]*string)
	srv.modules = sync.Map{}
	srv.plugins = sync.Map{}
	srv.pluginConfs = sync.Map{}
	srv.exports = sync.Map{}
	srv.notify = make(chan []byte, 1024)

	srv.loggerCmd = log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)
	srv.logger = srv.loggerCmd

	return &srv
}

func (srv *Server) Stop() {
	srv.stop = true
	close(srv.notify)
}

func (srv *Server) PrintVersion() {
	srv.logger.Printf("----------------------------------------------------------------------------------------------------------")
	srv.logger.Printf("-------------------                      backup agent started                          -------------------")
	srv.logger.Printf("----------------------------------------------------------------------------------------------------------")
	srv.logger.Printf("|                                                                                           |")
	srv.logger.Printf("| branch:%v commitId:%v ", GitBranch, GitCommitId)
	srv.logger.Printf("| repo %v", GitCommitRepo)
	srv.logger.Printf("| version %v", Version)
	srv.logger.Printf("| commitDate/buildDate %v , build Date: %v, User: %v, host=%v", GitCommitDate, BuildDate, BuildUser, BuildHost)
	srv.logger.Printf("|                                                                                           |")
	srv.logger.Printf("----------------------------------------------------------------------------------------------------------")
}

func (srv *Server) GetVersion() string {
	return Version + "-" + BuildDate + "-" + GitCommitId
}

func whiteFile(path string, value int) error {
    if err := ioutil.WriteFile(path, []byte(fmt.Sprintf("%d", value)), 0755); err != nil {
        return err
    }
	return nil
}

func (srv *Server) LimitResource() {
	if srv.conf.Resource.MaxThreads >= 1 {
		debug.SetMaxThreads(srv.conf.Resource.MaxThreads)
		srv.logger.Printf("[INFO] runtime limit max threads to %d\n", srv.conf.Resource.MaxThreads)
	}
	if srv.conf.Resource.MaxProcs >= 1 {
		runtime.GOMAXPROCS(srv.conf.Resource.MaxProcs)
		srv.logger.Printf("[INFO] runtime limit max cpu core to %d\n", srv.conf.Resource.MaxProcs)
	}
	if srv.conf.Resource.MaxMemory >= 1 {
		os.MkdirAll(cgroupRootPath, 0700)

		mPath := filepath.Join(cgroupRootPath, memoryLimitFile)
		err := whiteFile(mPath, srv.conf.Resource.MaxMemory * 1024 * 1024)
		if err != nil {
			srv.logger.Printf("[ERROR] white file %s failed :%s\n", mPath, err.Error())
			return
		}

		pPath := filepath.Join(cgroupRootPath, procsFile)
        err = whiteFile(pPath, os.Getpid())
		if err != nil {
			srv.logger.Printf("[ERROR] white file %s failed :%s\n", pPath, err.Error())
			return
		}

		srv.logger.Printf("[INFO] runtime limit max memory to %d MB\n", srv.conf.Resource.MaxMemory)
	}
}

func (srv *Server) StartPprof() {
	if srv.conf.Ctrl.PprofAddr == "" {
		srv.logger.Printf("[INFO] ignore pprof since addr in config is null\n")
		return
	}

	go func() {
		http.ListenAndServe(srv.conf.Ctrl.PprofAddr, nil)
	}()
	srv.logger.Printf("[INFO] pprof listen to %s\n", srv.conf.Ctrl.PprofAddr)
}

func (srv *Server) Init() error {
	srv.LoadConf()

	srv.InjectConf()

	srv.InitLog()

	srv.PrintVersion()

	srv.LimitResource()

	srv.StartPprof()

	if srv.conf.Log.ForceStdout {
		srv.logger = srv.loggerCmd
	}
	if srv.daemon {
		if !srv.conf.Log.ForceStdout {
			srv.logger = srv.loggerFile
		}
		srv.ctrl.Init(srv)
		srv.logger.Printf("[INFO] main Server init ctrl\n")
	}

	srv.instances = sync.Map{}

	srv.logger.Printf("[INFO] main Server init plugin begin")

	param := make(map[string]interface{})
	srv.plugins.Range(func(key, value interface{}) bool {
		plugin := value.(*PluginInfo)
		name := key.(string)
		if plugin.isDll {
			srv.logger.Printf("[INFO] main Server plugin [%s] in dll mode, init now\n", name)
			param["conf"] = plugin.conf
			param["binpath"] = srv.binPath
			param["imports"] = &srv.exports
			param["logger"] = srv.logger
			ctx, err := plugin.module.PluginABI.Init(param)
			if err != nil {
				srv.logger.Printf("[ERORR] main Server plugin [%s] init failed: %s\n", name, err.Error())
				os.Exit(1)
			}
			plugin.ctx = ctx
		}
		return true
	})

	srv.logger.Printf("[INFO] main Server init plugin done\n")
	return nil
}

func (srv *Server) LoadConf() {
	conf, err := ParseGlobalConf(srv.confPath)
	if err != nil {
		srv.logger.Printf("[ERROR] main config format error: %s\n", err.Error())
		os.Exit(1)
	}

	srv.daemon = true

	srv.logger.Printf("[INFO] main load conf begin\n")

	for _, pluginConf := range conf.Plugins {
		srv.pluginConfs.Store(pluginConf.Name, pluginConf)
	}

	for _, pluginConf := range conf.Plugins {
		srv.logger.Printf("[INFO] main Server load plugin [%s] conf\n", pluginConf.Name)
		_, err := srv.LoadPlugin(&pluginConf)
		if err != nil {
			srv.logger.Printf("[ERROR] main Server plugin [%s] load conf faield: %s\n", pluginConf.Name, err.Error())
			os.Exit(1)
		}
	}
	srv.conf = conf
	if srv.conf.RunTime.PidFile == "" {
		srv.pidFile = "/tmp/polarbackup.pid"
	} else {
		srv.pidFile = srv.conf.RunTime.PidFile
	}

	if srv.daemon {
		lockFile, ok := srv.conf.RunTime.LockFiles["daemon"]
		if !ok {
			lockFile = "/tmp/daemon-polarbackup.lock"
		}
		srv.lockFile = lockFile
	}
	srv.logger.Printf("[INFO] main load conf success\n")
}

func (srv *Server) InjectConf() error {
	target := flag.String("plugin", "", "run plugin")
	instance := flag.String("instance", "", "backup instance")
	_ = flag.String("config", "", "config")
	flag.Parse()

	if *target == "" && *instance == "" {
		return nil
	}

	srv.daemon = false

	srv.logger.Printf("[INFO] main inject conf running....\n")
	for plugName, input := range srv.confFlag {
		if *input == "" {
			continue
		}
		origin := make(map[string]interface{})
		_plugin, _ := srv.plugins.Load(plugName)
		plugin := _plugin.(*PluginInfo)
		err := json.Unmarshal(plugin.conf, &origin)
		if err != nil {
			return err
		}

		override := make(map[string]interface{})
		err = json.Unmarshal([]byte(*input), &override)
		if err != nil {
			return err
		}
		for k, v := range override {
			origin[k] = v
		}
		content, err := json.MarshalIndent(&origin, "", "\t")
		if err != nil {
			return err
		}
		plugin.conf = content
		srv.logger.Printf("[INFO] main plugin [%s] current config is \n%s\n", plugName, string(content))
	}

	if *target == "" {
		srv.logger.Printf("[ERROR] main no entry to run, exit now\n")
		os.Exit(1)
	}

	if *instance == "" {
		srv.logger.Printf("[ERROR] main no instance to run, exit now\n")
		os.Exit(1)
	}

	srv.logger.Printf("[INFO] main cmdline mode active, entry plugin is [%s], instance is [%s]\n", *target, *instance)
	srv.entry = *target
	srv.instance = *instance

	srv.lockFile = "/usr/local/polardb_o_backup_tool_current/" + srv.instance + "-polarbackup.lock"

	return nil
}

func (srv *Server) InitLog() {
	compress := srv.conf.Log.Compress
	fn := path.Join(srv.conf.Log.Logdir, "backup.log")
	size := srv.conf.Log.RotateSize
	count := srv.conf.Log.MaxCounts
	age := srv.conf.Log.MaxDays

	logger := &lumberjack.Logger{
		Filename:   fn,
		Compress:   compress,
		MaxSize:    size,
		MaxBackups: count,
		MaxAge:     age,
		LocalTime:  true,
	}
	srv.loggerFile = log.New(logger, "", log.LstdFlags|log.Lshortfile)
	srv.logger = srv.loggerFile
	srv.logger.Printf("[INFO] main init log success, logpath: %s\n", fn)
}

func (srv *Server) InitSingletonPluginInstance(name string, param map[string]interface{}) (interface{}, error) {
	if name == "" {
		srv.logger.Printf("[ERROR] main empty plugin cannot exec\n")
		return nil, fmt.Errorf("empty plugin entry")
	}

	plugConf := srv.conf.Plugins[name]

	_plugin, ok := srv.plugins.Load(plugConf.Name)
	if !ok {
		srv.logger.Printf("[ERROR] main plugin %s not exists\n", name)
		return nil, fmt.Errorf("plugin %s not exists", name)
	}
	plugin := _plugin.(*PluginInfo)
	param["conf"] = plugin.conf
	param["binpath"] = srv.binPath
	param["imports"] = &srv.exports
	param["notify"] = srv.notify
	param["logger"] = srv.logger
	stop := make(chan bool, 1)
	param["stop"] = stop

	extern := make(map[string]interface{})
	param["extern"] = extern
	srv.plugins.Range(func(k, v interface{}) bool {
		depName := k.(string)
		depPlugin := v.(*PluginInfo)
		kvs := make(map[string]interface{})
		err := json.Unmarshal(depPlugin.conf, &kvs)
		if err != nil {
			srv.logger.Printf("[ERROR] %s conf parse failed: %s\n", depName, err.Error())
			os.Exit(1)
		}
		extern[depName] = kvs
		srv.logger.Printf("[INFO] main plugin [%s] run with:\n%s\n", depName, string(depPlugin.conf))
		return true
	})

	ctx, err := plugin.module.PluginABI.Init(param)
	if err != nil {
		srv.logger.Printf("[INFO] main plugin [%s] init failed:\n%s\n", name, err.Error())
		return nil, err
	}
	plugin.ctx = ctx
	return ctx, nil
}

func (srv *Server) InitPluginInstance(msg *Message, param map[string]interface{}) (*PluginInstance, error) {
	plugConf := srv.conf.Plugins[msg.Plugin]

	_plugin, _ := srv.plugins.Load(plugConf.Name)
	plugin := _plugin.(*PluginInfo)
	param["conf"] = plugin.conf
	param["binpath"] = srv.binPath
	param["imports"] = &srv.exports
	param["notify"] = srv.notify
	param["logger"] = srv.logger
	stop := make(chan bool, 1)
	param["stop"] = stop

	ctx, err := plugin.module.PluginABI.Init(param)
	if err != nil {
		return nil, err
	}

	ins := &PluginInstance{
		plug: plugin,
		stop: stop,
		ctx:  ctx,
		id:   msg.InstanceID,
	}

	return ins, nil
}

func (srv *Server) LoadPlugin(conf *PluginConf) (*PluginInfo, error) {
	var module *ModuleInfo
	var plugin PluginInfo
	var err error

	_plugin, ok := srv.plugins.Load(conf.Name)
	if ok {
		return _plugin.(*PluginInfo), nil
	}

	srv.logger.Printf("[INFO] main load plugin [%s] config...\n", conf.Name)
	srv.confFlag[conf.Name] = flag.String(conf.Name, "", conf.Name+" cmdline args")

	conf.ModulePath = srv.binPath + "/" + conf.ModulePath
	conf.ConfPath = srv.binPath + "/" + conf.ConfPath

	_module, ok := srv.modules.Load(conf.ModulePath)
	if !ok {
		var m ModuleInfo
		module = &m
		srv.logger.Printf("[INFO] main init plugin [%s] module\n", conf.Name)
		err := module.ModuleInit(conf.ModulePath, conf.Exports)
		if err != nil {
			srv.logger.Printf("[ERROR] main init plugin [%s] module [%s] failed: %s\n", conf.Name, conf.ModulePath, err.Error())
			return nil, err
		}
		srv.logger.Printf("[INFO] main init plugin [%s] module [%s] success\n", conf.Name, conf.ModulePath)
		srv.exports.Store(conf.Name, module.Eat)
		srv.modules.Store(conf.ModulePath, module)
	} else {
		module = _module.(*ModuleInfo)
		srv.exports.Store(conf.Name, module.Eat)
		srv.logger.Printf("[INFO] main plugin [%s] module already inited, just load it\n", conf.Name)
	}

	plugin.module = module
	plugin.mode = conf.Mode
	if plugin.mode == "dll" || plugin.mode == "DLL" {
		plugin.isDll = true
	}
	plugin.deps = make(map[string]*PluginInfo)
	plugin.ctrl = make(map[string]interface{})
	plugin.conf, err = GetPluginConfig(conf)
	if err != nil {
		srv.logger.Printf("[ERROR] mian plugin [%s] config format error: %s\n", conf.Name, err.Error())
		os.Exit(1)
	}

	// add ctrl callbacks
	for path, funName := range conf.Ctrl {
		plugin.ctrl[path] = plugin.module.Eat[funName]
	}

	srv.logger.Printf("[INFO] main plugin [%s] init dependence plugin begin...\n", conf.Name)
	var depPlug *PluginInfo
	for _, depName := range conf.Dependence {
		srv.logger.Printf("[INFO] main plugin [%s] init dependence plugin\n", conf.Name)
		_depPlug, ok := srv.plugins.Load(depName)
		if !ok {
			_depConf, ok := srv.pluginConfs.Load(depName)
			if !ok {
				srv.logger.Printf("[ERROR] main plugin [%s] cannot find dependence plugin [%s]\n", conf.Name, depName)
				return nil, fmt.Errorf("%s cannot find deps plugin: %s", conf.Name, depName)
			}
			depConf := _depConf.(PluginConf)
			depPlug, err = srv.LoadPlugin(&depConf)
			if err != nil {
				srv.logger.Printf("[ERROR] main plugin [%s] dependence plugin [%s] load failed: %s\n", conf.Name, depName, err.Error())
				return nil, err
			}
		} else {
			srv.logger.Printf("[INFO] main plugin [%s] dependence plugin [%s] is already inited, just load it\n", conf.Name, depName)
			depPlug = _depPlug.(*PluginInfo)
		}
		plugin.deps[depName] = depPlug
	}
	srv.plugins.Store(conf.Name, &plugin)
	srv.logger.Printf("[INFO] main plugin [%s] init dependence plugin success: %p\n", conf.Name, &plugin)
	return &plugin, nil
}

func (srv *Server) Start() {
	pid := os.Getpid()
	ioutil.WriteFile(srv.pidFile, []byte(strconv.Itoa(pid)), 0644)

	if srv.daemon {
		srv.logger.Printf("[INFO] main Server start ctrl")

		pb.GetCMCtrlServerInstance(srv.conf.Ctrl.CMPort, srv.GetVersion(), srv.logger)

		if srv.conf.Ctrl.EnableBlockServer {
			srv.startBlockFileServer()
		}

		srv.ctrl.Start()
		for !srv.stop {
			srv.logger.Printf("[INFO] main Server wait for async msg\n")
			msg, err := RecvMsg(srv.notify)
			if err != nil {
				srv.logger.Printf("[ERROR] main recv msg failed\n")
				continue
			}
			srv.logger.Printf("[INFO] main recv a new msg [%s.%s]\n", msg.Plugin, msg.Cmd)
			go func() {
				_ = srv.MsgAction(msg, false)
			}()
		}
	} else {
		srv.logger.Printf("[INFO] main Cmdline Mode Running ...\n")
		var msg *Message
		pluginName := srv.entry
		err := srv.ConfAction(srv.entry)
		if err == nil {
			return
		}
		for {
			if err == syscall.EAGAIN {
				msg, err = RecvMsg(srv.notify)
				if err != nil {
					srv.logger.Printf("[ERROR] main recv msg failed\n")
					break
				}
				pluginName = msg.Plugin
				err = srv.MsgAction(msg, true)
			} else {
				srv.logger.Printf("[ERROR] main exec plugin [%s] failed %s\n", pluginName, err.Error())
				os.Exit(-1)
				break
			}
		}
	}
	return
}

func (srv *Server) ConfAction(plugName string) error {
	param := make(map[string]interface{})
	param["cmdline"] = true
	_, err := srv.InitSingletonPluginInstance(plugName, param)
	if err != nil {
		return err
	}

	_plug, _ := srv.plugins.Load(plugName)
	plug := _plug.(*PluginInfo)

	srv.logger.Printf("[INFO] main [%s] run\n", plugName)
	err = plug.module.PluginABI.Run(plug.ctx, param)
	if err != nil {
		srv.logger.Printf("[ERROR] main [%s] Run failed: %s\n", plugName, err.Error())
		fmt.Printf("[ERROR] main [%s] Run failed: %s\n", plugName, err.Error())
	} else {
		srv.logger.Printf("[INFO] main [%s] Run success\n", plugName)
		fmt.Printf("[INFO] main [%s] Run success\n", plugName)
	}
	return err
}

func (srv *Server) MsgAction(msg *Message, cmdline bool) error {
	var err error

	srv.logger.Printf("[INFO] main [%s.%s] preparing\n", msg.Plugin, msg.Cmd)
	_plug, ok := srv.plugins.Load(msg.Plugin)
	if !ok {
		srv.logger.Printf("[ERROR] main plugin %s not exists\n", msg.Plugin)
		return fmt.Errorf("plugin %s not exists", msg.Plugin)
	}
	plug := _plug.(*PluginInfo)

	var ctx interface{}
	var ins *PluginInstance
	param := make(map[string]interface{})

	isInit := false
	if !plug.isDll {
		srv.logger.Printf("[INFO] main [%s] is instance mode, find target instance [%s] run cmd [%s]\n", msg.InstanceID, msg.Plugin, msg.Cmd)
		srv.mutex.Lock()
		_ins, ok := srv.instances.Load(msg.InstanceID)
		if !ok {
			srv.logger.Printf("[INFO] main [%s.%s] cannot find instance [%s], create a new instance\n", msg.InstanceID, msg.Plugin, msg.Cmd)
			param["cmdline"] = cmdline
			ins, err = srv.InitPluginInstance(msg, param)
			srv.logger.Printf("[INFO] main [%s.%s] add new instance [%s]\n", msg.InstanceID, msg.Plugin, msg.Cmd)
			srv.instances.Store(msg.InstanceID, ins)
			if err != nil {
				srv.logger.Printf("[ERROR] main [%s.%s] create new instance failed: %s \n", msg.Plugin, msg.Cmd, err.Error())
				srv.mutex.Unlock()
				return err
			}
			ctx = ins.ctx
			isInit = true
			srv.logger.Printf("[INFO] main [%s.%s] Init new instance success\n", msg.Plugin, msg.Cmd)
		} else {
			ins = _ins.(*PluginInstance)
			ctx = ins.ctx
			if checkOnce(msg.Cmd) {
				err = errors.New("can not execute " + msg.Cmd + " again in same instance ctx")
				srv.logger.Printf("[ERROR] main [%s.%s] obtain old instance %s failed: %s \n", msg.Plugin, msg.Cmd, msg.InstanceID, err.Error())
				srv.mutex.Unlock()
				return err
			}
		}
		srv.mutex.Unlock()
	} else {
		srv.logger.Printf("[INFO] main [%s] is daemon mode, use the global instance run cmd [%s]\n", msg.Plugin, msg.Cmd)
		ctx = plug.ctx
	}

	param["extern"] = msg.Data

	err = plug.module.PluginABI.Run(ctx, param)
	if err != nil {
		srv.logger.Printf("[ERROR] main instance [%s.%s] Run failed: %s\n", msg.Plugin, msg.Cmd, err.Error())
		// return err
	} else {
		srv.logger.Printf("[INFO] main [%s.%s] Run success\n", msg.Plugin, msg.Cmd)
	}

	/*
	 * Who init instance, who take charge of removing it. Otherwise, it will be complicated and confused.
	 */
	if !plug.isDll && checkExit(msg.Cmd) && isInit {
		plug.module.PluginABI.Exit(ctx)
		// clean up instance
		srv.logger.Printf("[INFO] main [%s.%s] remove instance [%s]\n", msg.Plugin, msg.Cmd, msg.InstanceID)
		if _, ok := srv.instances.Load(ins.id); ok {
			// ins.stop <- true
			srv.instances.Delete(ins.id)
		}
	}

	// for cmdline output
	if err != nil {
		fmt.Printf("[ERROR] main [%s.%s] Run failed: %s\n", msg.Plugin, msg.Cmd, err.Error())
	} else {
		fmt.Printf("[INFO] main [%s.%s] Run success\n", msg.Plugin, msg.Cmd)
	}
	return err
}

func checkOnce(cmd string) bool {
	return cmd == "backup" || cmd == "restore"
}

func checkExit(cmd string) bool {
	if cmd == "limit" {
		return false
	}

	if cmd == "updatetopo" {
		return false
	}

	return true
}

func (srv *Server) Exit() error {
	srv.instances.Range(func(_, _ins interface{}) bool {
		ins := _ins.(*PluginInstance)
		ins.stop <- true
		srv.instances.Delete(ins.id)
		return true
	})
	return nil
}

func main() {
	srv := NewServer()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		_ = <-sigs
		srv.Stop()
	}()

	srv.Init()

	defer func() {
		if r := recover(); r != nil {
			errStr := string(debug.Stack())
			fmt.Printf("backupctl panic from:\n%s", errStr)
			srv.logger.Printf("backupctl panic from:\n%s", errStr)
		}
	}()

	if srv.conf.RunTime.Exclusive {
		ok, err := LockFile(srv.lockFile)
		if !ok {
			if err != nil {
				fmt.Printf("lock %s failed: %s\n", srv.lockFile, err.Error())
			} else {
				fmt.Printf("cannot get file lock: %s\n", srv.lockFile)
			}
			os.Exit(1)
		}
	}

	srv.Start()
	srv.Exit()

	if srv.conf.RunTime.Exclusive {
		UnlockFile(srv.lockFile)
	}
}

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

/*
`
"plugins: "xxx,so", "xxx.so"
plugin_xxx.conf {
    "key": "values"
}
`
*/

type PluginConf struct {
	ModulePath string `json:"ModulePath"`
	// lua, golang
	ModuleMode string              `json:"ModuleMode"`
	ConfPath   string              `json:"ConfPath"`
	Name       string              `json:"Name"`
	Exports    []string            `json:"Exports"`
	Imports    map[string][]string `json:"Imports"`
	// dep plugin name
	Dependence []string          `json:"Dependence"`
	Ctrl       map[string]string `json:"Ctrl"`
	Mode       string            `json:"Mode"`
}

type CtrlConf struct {
	ListenAddr        string `json:"ListenAddr"`
	Enable            bool   `json:"Enable"`
	CMPort            string `json:"CMPort"`
	PprofAddr         string `json:"PprofAddr"`
	EnableBlockServer bool   `json:"EnableBlockServer"`
	BlockServerPort   string `json:"BlockServerPort"`
	BlockBackupDir    string `json:"BlockBackupDir"`
}

type LogConf struct {
	Logdir      string `json:"Logdir"`
	Compress    bool   `json:"Compress"`
	RotateSize  int    `json:"RotateSize"`
	MaxCounts   int    `json:"MaxCounts"`
	MaxDays     int    `json:"MaxDays"`
	ForceStdout bool   `json:"ForceStdout"`
}

type RunTimeConf struct {
	PidFile   string            `json:"PidFile"`
	LockFiles map[string]string `json:"LockFiles"`
	Exclusive bool              `json:"Exclusive"`
}

type ResourceConf struct {
	MaxThreads int `json:"MaxThreads"`
	MaxMemory  int `json:"MaxMemory"`
	MaxProcs   int `json:"MaxProcs"`
}

type GlobalConf struct {
	Plugins  map[string]PluginConf `json:"Plugins"`
	Ctrl     CtrlConf              `json:"Ctrl"`
	Log      LogConf               `json:"Log"`
	RunTime  RunTimeConf           `json:"RunTime"`
	Resource ResourceConf          `json:"Resource"`
}

func ParseGlobalConf(path string) (*GlobalConf, error) {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read global conf [%s] failed: %s", path, err.Error())
	}

	var conf GlobalConf
	err = json.Unmarshal(content, &conf)
	if err != nil {
		return nil, fmt.Errorf("parse global conf [%s] failed: %s", path, err.Error())
	}

	return &conf, nil
}

func ParsePluginConf(path string) (PluginConf, error) {
	var conf PluginConf
	_, err := ioutil.ReadFile(path)
	if err != nil {
		return conf, fmt.Errorf("plugin conf: %s cannot found", path)
	}
	return conf, nil
}

func GetPluginConfig(plugin *PluginConf) ([]byte, error) {
	content, err := ioutil.ReadFile(plugin.ConfPath)
	if err != nil {
		return nil, fmt.Errorf("plugin: %s, read conf %s error: %s", plugin.Name, plugin.ConfPath, err.Error())
	}
	return content, nil
}

func test() {
	conf, _ := ParseGlobalConf("pipeline.conf")
	for _, p := range conf.Plugins {
		fmt.Printf("%s\n%+v\n", p.Name, p)
	}
}

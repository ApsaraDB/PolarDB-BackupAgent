/*
 * Copyright (c) 2018. Alibaba Cloud, All right reserved.
 * This software is the confidential and proprietary information of Alibaba Cloud ("Confidential Information").
 * You shall not disclose such Confidential Information and shall use it only in accordance with the terms of
 * the license agreement you entered into with Alibaba Cloud.
 */

package main

import (
	"errors"
	"fmt"
	"plugin"
)

// PluginInterface plugin common
type PluginInterface struct {
	Init func(interface{}) (interface{}, error)
	Run  func(interface{}, interface{}) error
	Exit func(interface{}) error
}

// ModuleInfo a module struct
type ModuleInfo struct {
	plugin    *plugin.Plugin
	PluginABI PluginInterface
	Eat       map[string]interface{} // module-name.FuncName as key
	Iat       map[string]interface{} // module-name.FuncName as key
}

func (module *ModuleInfo) loadFunction(exports []string) error {
	for _, identifier := range exports {
		fn, err := module.plugin.Lookup(identifier)
		if err != nil {
			return err
		}
		module.Eat[identifier] = fn
	}
	return nil
}

func (module *ModuleInfo) modulePrepareGolang(path string) error {
	p, err := plugin.Open(path)
	if err != nil {
		return fmt.Errorf("modulePrepareGolang open error, path:%s, err:%s", path, err.Error())
	}

	module.PluginABI.Init = nil
	module.PluginABI.Run = nil
	module.PluginABI.Exit = nil

	var ok bool

	initPlugin, err := p.Lookup("PluginInit")
	if err != nil {
		return err
	}

	module.PluginABI.Init, ok = initPlugin.(func(interface{}) (interface{}, error))
	if !ok {
		return errors.New("PluginInit not match abi")
	}

	runPlugin, err := p.Lookup("PluginRun")
	if err != nil {
		return errors.New("PluginRun lookup error")
	}
	module.PluginABI.Run, ok = runPlugin.(func(interface{}, interface{}) error)
	if !ok {
		return errors.New("PluginRun not match abi")
	}

	exitPlugin, err := p.Lookup("PluginExit")
	if err != nil {
		return err
	}

	module.PluginABI.Exit, ok = exitPlugin.(func(interface{}) error)
	if !ok {
		return errors.New("PluginExit not match abi")
	}

	module.plugin = p
	return nil
}

// ModuleInit init a module
func (module *ModuleInfo) ModuleInit(path string, exports []string) error {
	module.PluginABI = PluginInterface{}
	module.Eat = make(map[string]interface{})
	module.Iat = make(map[string]interface{})

	if err := module.modulePrepareGolang(path); err != nil {
		return err
	}
	if err := module.loadFunction(exports); err != nil {
		return err
	}

	return nil
}

// ModuleExit exit module
func (module *ModuleInfo) ModuleExit() {
}

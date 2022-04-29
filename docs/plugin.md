# How to build a new storage plugin

PolarDB Backup Agent is flexible to extend the new plugins. Therefore, you can create a new storage plugin for specify storage need. And it wouldn't change any logic of backup and recovery. You can following the follow step to create a new storage plugin.

- create a new plugin folder under plugins folder, create $pluginname.go and Makefile
```bash
plugins/$pluginname/$pluginname.go
plugins/$pluginname/Makefile
```

- define the function mapping in ExportMapping function, for example:
```bash
	funcs["init"] = HTTPInit
	funcs["fini"] = HTTPFini
	funcs["unlink"] = HTTPUnlink
	funcs["open"] = HTTPOpen
	funcs["create"] = HTTPCreate
	funcs["close"] = HTTPClose
	funcs["read"] = HTTPRead
	funcs["write"] = HTTPWrite
	funcs["chdir"] = HTTPEmpty
	funcs["readdir"] = HTTPReaddir
	funcs["readdir_r"] = HTTPReaddir
	funcs["rmdir"] = HTTPRemoveAll
	funcs["size"] = HTTPSize
	funcs["exist"] = HTTPExist
```

- register the new plugin in loader.conf, and create $pluginname.conf in the same directory
```bash
        "$pluginname": {
            "ModulePath": "$pluginname.so",
            "ModuleMode": "golang",
            "ConfPath": "$pluginname.conf",
            "Name": "$pluginname",
            "Exports": ["ExportMapping"],
            "Mode": "DLL"
        }
```

- define the plugin in Locations struct in pipeline_msg.go
```bash
type Locations struct {
	S3    S3    `json:"S3"`
	FTP   FTP   `json:"FTP"`
	Local Local `json:"Local"`
	DBS   DBS   `json:"DBS"`
	HTTP  HTTP  `json:"HTTP"`
    $pluginname $pluginname `json:"$pluginname"`
}
```

- modify the following functions in pipeline_common.go to add the new plugin
```bash
getLocation()
storageToMap()
storageToLocation()
```

- modify the following functions in pipeline_ctl.go to add the new plugin
```bash
StartFullBackupCallback()
StartIncreBackupCallback()
RecoveryCallback()
```
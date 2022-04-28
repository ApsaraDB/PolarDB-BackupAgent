package main

const (
	BackupTypeFull        = "Full"
	BackupTypeIncremental = "Incremental"
)

const (
	SystemInternalError     = 0
	BackupExecuteNotExist   = 1
	BackupExecuteEnded      = 2
	MetadataUpdateFailed    = 3
	DBClusterNotExist       = 4
	IllegalParameter        = 5
	BackupLimitUpdateFailed = 6
)

type BackupRequest struct {
	BackupMachineList []string `json:"BackupMachineList"`
	InstanceID        string   `json:"InstanceID"`
	BackupID          string   `json:"BackupID"`
	BackupJobID       string   `json:"BackupJobID"`
	WorkerCount       int      `json:"WorkerCount"`
	BackupType        string   `json:"BackupType"`
	UseBlock          bool     `json:"UseBlock"`
	PGType            string   `json:"PGType"`
	StartOffset       int64    `json:"StartOffset"`
	CallbackURL       string   `json:"CallbackURL"`
	DBSCallbackURL    string   `json:"DBSCallbackURL"`
	//PFSMode            string   `json:"PFSMode"`
	PgDBConf           CommonDBConf       `json:"PgDBConf"`
	BackupAccount      BackupAccount      `json:"BackupAccount"`
	MetaAccount        MetaAccount        `json:"MetaAccount"`
	Filesystem         string             `json:"Filesystem"`
	BackupStorageSpace BackupStorageSpace `json:"BackupStorageSpace"`
	BackupPBD          string             `json:"BackupPBD"`
	BackupFolder       string             `json:"BackupFolder"`
	DBClusterMetaDir   string             `json:"DBClusterMetaDir"`
	MaxBackupSpeed     int                `json:"MaxBackupSpeed"`
	ManagerAddr        string             `json:"ManagerAddr"`
	DBNodes            []string           `json:"DBNodes"`
	EnableEncryption   bool               `json:"EnableEncryption"`
	EncryptionPassword string             `json:"EncryptionPassword"`
}

type BackupStorageSpace struct {
	StorageType string    `json:"StorageType"`
	NameSpace   string    `json:"NameSpace"`
	Locations   Locations `json:"Locations"`
}

type Locations struct {
	S3    S3    `json:"S3"`
	FTP   FTP   `json:"FTP"`
	Local Local `json:"Local"`
	DBS   DBS   `json:"DBS"`
	HTTP  HTTP  `json:"HTTP"`
}

type HTTP struct {
	Endpoint string `json:"Endpoint"`
}

type DBS struct {
	Endpoint  string `json:"Endpoint"`
	Namespace string `json:"Namespace"`
}

type S3 struct {
	Endpoint  string `json:"Endpoint"`
	Bucket    string `json:"Bucket"`
	Accesskey string `json:"Accesskey"`
	Secretkey string `json:"Secretkey"`
	Region    string `json:"Region"`
	Secure    bool   `json:"Secure"`
}

type FTP struct {
	Address   string `json:"Address"`
	Port      string `json:"Port"`
	Path      string `json:"Path"`
	Anonymous bool   `json:"Anonymous"`
	Username  string `json:"Username"`
	Password  string `json:"Password"`
}

type Local struct {
	Path string `json:"Path"`
}

type StopBackupRequest struct {
	BackupMachineList []string      `json:"BackupMachineList"`
	InstanceID        string        `json:"InstanceID"`
	BackupID          string        `json:"BackupID"`
	BackupJobID       string        `json:"BackupJobID"`
	BackupType        string        `json:"BackupType"`
	Filesystem        string        `json:"Filesystem"`
	BackupAccount     BackupAccount `json:"BackupAccount"`
	MetaAccount       MetaAccount   `json:"MetaAccount"`
	PGType            string        `json:"PGType"`
}

type BackupMetaInfo struct {
	File        string `json:"Files"` // meta file name
	StartTime   int64  `json:"StartTime"`
	EndTime     int64  `json:"EndTime"`
	Location    string `json:"Location"`
	InstanceID  string `json:"InstanceID"`
	BackupID    string `json:"BackupID"`
	BackupJobID string `json:"BackupJobID"`
	Status      string `json:"Status"`
	IsBlock     bool   `json:"IsBlock"`
}

type WalLogMetaInfo struct {
	File        string `json:"Files"`
	FileSize    int64  `json:"FileSize"`
	TimeMode    string `json:"TimeMode"`
	StartTime   int64  `json:"StartTime"`
	EndTime     int64  `json:"EndTime"`
	Location    string `json:"Location"`
	InstanceID  string `json:"InstanceID"`
	BackupID    string `json:"BackupID"`
	BackupJobID string `json:"BackupJobID"`
	Status      string `json:"Status"`
}

type RecoveryRequest struct {
	BackupMachineList  []string           `json:"BackupMachineList"`
	CallbackURL        string             `json:"CallbackURL"`
	InstanceID         string             `json:"InstanceID"` // target instance id
	BackupJobID        string             `json:"BackupJobID"`
	RecordID           string             `json:"RecordID"`
	WorkerCount        int                `json:"WorkerCount"`
	Full               BackupInfo         `json:"Full"`
	Incremental        BackupInfo         `json:"Incremental"`
	Stage              string             `json:"Stage"`
	RecoveryTime       int64              `json:"RecoveryTime"`
	RecoveryMode       string             `json:"RecoveryMode"`
	UseBlock           bool               `json:"UseBlock"`
	RecoveryFolder     string             `json:"RecoveryFolder"`
	Filesystem         string             `json:"Filesystem"`
	BackupStorageSpace BackupStorageSpace `json:"BackupStorageSpace"`
	BackupPBD          string             `json:"BackupPBD"`
	BackupAccount      BackupAccount      `json:"BackupAccount"`
	MetaAccount        MetaAccount        `json:"MetaAccount"`
	FullBackup         BackupMetaInfo     `json:"FullBackup"`
	BlockBackups       []BackupMetaInfo   `json:"BlockBackups"`
	BlockBackupIndex   int                `json:"BlockBackupIndex"`
	IncreBackups       []WalLogMetaInfo   `json:"IncreBackups"`
	BackupMetaSource   string             `json:"BackupMetaSource"`
	PGType             string             `json:"PGType"`
	DBClusterMetaDir   string             `json:"DBClusterMetaDir"`
	EnableEncryption   bool               `json:"EnableEncryption"`
	EncryptionPassword string             `json:"EncryptionPassword"`
}

type CalRecoveryRequest struct {
	ManagerAddr        string             `json:"ManagerAddr"`
	InstanceID         string             `json:"InstanceID"`
	BackupID           string             `json:"BackupID"`
	StartTime          int64              `json:"StartTime"`
	EndTime            int64              `json:"EndTime"`
	RecoveryTime       int64              `json:"RecoveryTime"`
	RecoveryMode       string             `json:"RecoveryMode"`
	BackupStorageSpace BackupStorageSpace `json:"BackupStorageSpace"`
	FullBackup         BackupMetaInfo     `json:"FullBackup"`
	BlockBackups       []BackupMetaInfo   `json:"BlockBackups"`
	IncreBackups       []WalLogMetaInfo   `json:"IncreBackups"`
}

type WalHelperRequest struct {
	BackupFolder   string      `json:"BackupFolder"`
	CurLogFile     LogFileInfo `json:"CurLogFile"`
	BlockBackupDir string      `json:"BlockBackupDir"`
	InstanceID     string      `json:"InstanceID"`
	Action         string      `json:"Action"`
}

type UpdateTopologyRequest struct {
	InstanceID  string `json:"InstanceID"`
	BackupID    string `json:"BackupID"`
	BackupJobID string `json:"BackupJobID"`
	Master      string `json:"Master"`
}

type LimitRequest struct {
	InstanceID          string   `json:"InstanceID"` // target instance id
	BackupID            string   `json:"BackupID"`
	BackupJobID         string   `json:"BackupJobID"`
	MaxBackupSpeed      int      `json:"MaxBackupSpeed"`
	MaxTotalBackupSpeed int      `json:"MaxTotalBackupSpeed"`
	BackupMachineList   []string `json:"BackupMachineList"`
}

type DescribeRequest struct {
	BackupType string `json:"BackupType"`
	InstanceID string `json:"InstanceID"`
}

type RecoveryStatus struct {
	BackupJobID      string
	BackupID         string
	InstanceID       string

	RecoverySize     int64
	RecoveredSize    int64

	FullRecoveredSize int64

	Process          int
	Status           string
	Error            string

	WalsNum          int
}

type BackupInfo struct {
	InstanceID  string `json:"InstanceID"`
	BackupID    string `json:"BackupID"`
	BackupJobID string `json:"BackupJobID"`
}

type LogFileInfo struct {
	FileName    string `json:"FileName"`
	FileSize    int64  `json:"FileSize"`
	BackupSpeed int64  `json:"BackupSpeed"`
	TimeMode    string `json:"TimeMode"`
	StartTime   int64  `json:"StartTime"`
	EndTime     int64  `json:"EndTime"`
	StartOffset int64  `json:"StartOffset"`
	EndOffset   int64  `json:"EndOffset"`
	PrevOffset  int64  `json:"PrevOffset"`
}

type ResponseData struct {
	Code  int    `json:"code"`
	Error string `json:"error"`
	Data  string `json:"data"`
}

type DBSResponseData struct {
	Code  int    `json:"code"`
	Error string `json:"msg"`
}

type StatusCallback struct {
	InstanceID  string `json:"InstanceID"`
	BackupID    string `json:"BackupID"`
	BackupJobID string `json:"BackupJobID"`
	Process     int    `json:"Process"`
	Status      string `json:"Status"`
	StartTime   int    `json:"StartTime"`
	EndTime     int    `json:"EndTime"`
	StartOffset int    `json:"StartOffset"`
	EndOffset   int    `json:"EndOffset"`
	NextOffset  int64  `json:"NextOffset"`
}

////////////////////////////////////////////////////////////////////

type RunningStatus struct {
	Action           string `json:"Action"`
	CurrentNodeTasks int    `json:"CurrentNodeTasks"`
	RealbkSize       int64  `json:"RealbkSize"`
	BackupSize       int64  `json:"BackupSize"`
	BackedSize       int64  `json:"BackedSize"`
	AllFilesCount    int    `json:"AllFiles"`
	Upload           int    `json:"Upload"`
	Download         int32  `json:"Download"`

	CurLogFile   LogFileInfo `json:"CurLogFile"`
	UsingLogFile LogFileInfo `json:"UsingLogFile"`

	Location   string      `json:"Location"`

	Error       string       `json:"Error"`
	Stage       string       `json:"Stage"`
	Node        string       `json:"Node"`
	ID          string       `json:"ID"`
	PgDBConf    CommonDBConf `json:"PgDBConf"`
	MetaAccount MetaAccount  `json:"MetaAccount"`

	StartTime int64 `json:"StartTime"`
	EndTime   int64 `json:"EndTime"`

	Pbd               string   `json:"Pbd"`
	BackupMachineList []string `json:"BackupMachineList"`

	BackupJobID    string `json:"BackupJobID"`
	BackupID       string `json:"BackupID"`
	InstanceID     string `json:"InstanceID"`
	Callback       string `json:"Callback"`
	DBSCallbackURL string `json:"DBSCallbackURL`

	Backend  string `json:"Backend"`
	Type     string `json:"Type"`
	UseBlock bool   `json:"UseBlock"`
}

type BackupJob struct {
	Gateway  string `json:"Gateway"`
	Backend  string `json:"Backend"`
	Frontend string `json:"Frontend"`

	WorkerCount    int      `json:"WorkerCount"`
	Location       string   `json:"Location"`
	Action         string   `json:"Action"`
	CallbackURL    string   `json:"CallbackURL`
	DBSCallbackURL string   `json:"DBSCallbackURL`
	Pbd            string   `json:"Pbd"`
	BackupNodes    []string `json:"BackupNodes"`
	Plugin         string   `json:"Plugin"`
	RecoveryType   string   `json:"RecoveryType"`
	UseBlock       bool     `json:"UseBlock"`
	PGType         string   `json:"PGType"`

	BackupJobID        string        `json:"BackupJobID"`
	BackupID           string        `json:"BackupID"`
	InstanceID         string        `json:"InstanceID"`
	LogFiles           []string      `json:"LogFiles"`
	BackupAccount      BackupAccount `json:"BackupAccount"`
	MetaAccount        MetaAccount   `json:"MetaAccount"`
	DBClusterMetaDir   string        `json:"DBClusterMetaDir"`
	RecoveryFolder     string        `json:"RecoveryFolder"`
	BackupFolder       string        `json:"BackupFolder"`
	MaxBackupSpeed     int           `json:"MaxBackupSpeed"`
	Master             string        `json:"Master"`
	ManagerAddr        string        `json:"ManagerAddr"`
	DBNodes            []string      `json:"DBNodes"`
	EnableEncryption   bool          `json:"EnableEncryption"`
	EncryptionPassword string        `json:"EncryptionPassword"`
}

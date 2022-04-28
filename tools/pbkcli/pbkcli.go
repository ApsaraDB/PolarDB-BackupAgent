package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// private
var (
	id         string
	mode       string
	backupConf BackupConf

	CfgFile string
	RootCmd = NewPbkcliCommand()
)

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Backup PolarDB Cluster",
	Long: `Backup PolarDB Cluster.

	For example:

	pdbcli backup start --mode=full
	pdbcli backup stop --id=20211029
	pdbcli backup recover
	pdbcli backup status`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Debug("backup called")
		if len(args) <= 0 {
			fmt.Println(`Error: You must specify backup action subcmd, valid choises: start/stop/recover/status`)
		} else {
			switch args[0] {
			case "start", "stop", "status", "recover":
				break
			default:
				fmt.Println(`Error: You must specify backup action subcmd, valid choises: start/stop/recover/status`)
			}
		}
	},
}

var backupStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Backup start",
	Long: `Backup start.
	
	For example:

	pdbcli backup start --mode=full`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Debug("backup start called")
		err := initConfig()
		if err != nil {
			er(err.Error())
		}

		startBackup(args, cmd)
	},
}

var backupStopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Backup stop",
	Long: `Backup stop.
	
	For example:

	pdbcli backup stop --id=20211031`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Debug("backup stop called")
		err := initConfig()
		if err != nil {
			er(err.Error())
		}

		stopBackup(args, cmd)
	},
}

var backupStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Backup status",
	Long: `Backup status.
	
	For example:

	pdbcli backup status --id=20211031`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Debug("backup status called")
		err := initConfig()
		if err != nil {
			er(err.Error())
		}

		statusBackup(args, cmd)
	},
}

var backupRecoverCmd = &cobra.Command{
	Use:   "recover",
	Short: "Backup recover",
	Long: `Backup recover.
	
	For example:

	pdbcli backup recover --id=20211031`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Debug("backup recover called")
		err := initConfig()
		if err != nil {
			er(err.Error())
		}

		recoverBackup(args, cmd)
	},
}

func init() {
	RootCmd.PersistentFlags().StringVar(&CfgFile, "config", "./sample.backup.conf", "Specify Config file.")

	RootCmd.AddCommand(backupStartCmd)
	RootCmd.AddCommand(backupStopCmd)
	RootCmd.AddCommand(backupStatusCmd)
	RootCmd.AddCommand(backupRecoverCmd)

	RootCmd.PersistentFlags().StringVarP(&mode, "mode", "", "", "Backup mode enum: incremental/full.")
	RootCmd.PersistentFlags().StringVarP(&id, "id", "", "", "Backup ID string, e.g. Full-20210310")
}

func initConfig() error {

	content, err := ioutil.ReadFile(CfgFile)
	if err != nil {
		return fmt.Errorf("read conf [%s] failed: %s", CfgFile, err.Error())
	}

	err = json.Unmarshal(content, &backupConf)
	if err != nil {
		return fmt.Errorf("parse conf [%s] failed: %s", CfgFile, err.Error())
	}

	return nil
}

func main() {
	Execute()
}

func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Printf("execute failed: %s\n", err.Error())
	}
}

func NewPbkcliCommand() *cobra.Command {

	cmds := cobra.Command{
		Use:   "pbkcli",
		Short: "The PolarDB Backup Agent command line interface (pbkcli).",
		Long:  `The PolarDB Backup Agent command line interface (pbkcli) lets you backup and recover PolarDB clusters.`,
	}

	return &cmds
}

func initBackupInfo(backupRequest BackupRequest) BackupRequest {
	backupRequest.BackupMachineList = append(backupRequest.BackupMachineList, backupConf.Service)

	backupRequest.Filesystem = backupConf.Filesystem

	backupRequest.BackupStorageSpace = backupConf.BackupStorageSpace

	backupRequest.InstanceID = backupConf.InstanceID

	if mode == "" {
		er("Please specify backup mode with parameter '--mode'.")
	}
	mode = strings.ToUpper(mode[:1]) + strings.ToLower(mode[1:])
	if mode != "Full" && mode != "Incremental" && mode != "Delta" {
		er("Only 'full' 'delta' or 'incremental' is accepted by parameter '--mode'.")
	}

	useBlock := false
	bktype := mode
	// Delta is considered as logic Full backup for backup agent
	if bktype == "Delta" {
		bktype = "Full"
		useBlock = true
	}

	backupRequest.BackupType = bktype
	backupRequest.BackupJobID = BACKUP_JOB_ID
	backupRequest.UseBlock = useBlock

	backupRequest.BackupPBD = backupConf.BackupPBD

	backupRequest.WorkerCount = backupConf.WorkerCount

	backupRequest.MaxBackupSpeed = backupConf.MaxBackupSpeed

	backupRequest.EnableEncryption = backupConf.EnableEncryption

	backupRequest.EncryptionPassword = backupConf.EncryptionPassword

	if mode == "Full" || mode == "Delta" {
		backupRequest.BackupID = mode + "-" + time.Now().Format("20060102150405")
	}
	if mode == "Incremental" {
		backupRequest.BackupID = DEFAULT_INCREMENTAL_BACKUPID
	}

	backupRequest.BackupAccount = backupConf.BackupAccount

	return backupRequest
}

func startBackup(args []string, backupStartCmd *cobra.Command) {
	var backupRequest BackupRequest
	backupRequest = initBackupInfo(backupRequest)

	InitService(backupConf.Service)
	_, err := ServiceStartBackup(backupRequest)
	if err != nil {
		er(err.Error())
	}

	fmt.Printf("%s backup start success, backup ID is: %s\n", mode, backupRequest.BackupID)
}

func stopBackup(args []string, backupStopCmd *cobra.Command) {
	var backupRequest BackupRequest

	backupRequest.BackupMachineList = append(backupRequest.BackupMachineList, backupConf.Service)

	backupRequest.InstanceID = backupConf.InstanceID

	if mode == "" {
		er("Please specify backup mode with parameter '--mode'.")
	}
	mode = strings.ToUpper(mode[:1]) + strings.ToLower(mode[1:])
	if mode != "Full" && mode != "Incremental" {
		er("Only 'full' or 'incremental' is accepted by parameter '--mode'.")
	}
	backupRequest.BackupType = mode

	if mode == "Incremental" {
		if id == "" {
			id = DEFAULT_INCREMENTAL_BACKUPID
		}
		backupRequest.BackupID = id
	}

	backupRequest.BackupJobID = BACKUP_JOB_ID
	backupRequest.PGType = BACKUP_PG_TYPE

	InitService(backupConf.Service)
	_, err := ServiceStopBackup(backupRequest)
	if err != nil {
		er(err.Error())
	}

	if mode == "Incremental" {
		fmt.Printf("%s backup stop success, backup ID is: %s\n", mode, backupRequest.BackupID)
	} else {
		fmt.Sprintf("%s backup stop success\n", mode)
	}
}

func statusBackup(args []string, backupStatusCmd *cobra.Command) {
	if mode == "" {
		er("Please specify backup mode with parameter '--mode'.")
	}
	mode = strings.ToUpper(mode[:1]) + strings.ToLower(mode[1:])
	if mode != "Full" && mode != "Incremental" {
		er("Only 'full' or 'incremental' is accepted by parameter '--mode'.")
	}

	InitService(backupConf.Service)
	body, err := ServiceStatusBackup(mode)
	if err != nil {
		er(err.Error())
	}

	fmt.Println(body)
}

func recoverBackup(args []string, backupRecoverCmd *cobra.Command) {
	var recoveryRequest RecoveryRequest

	recoveryRequest.BackupMachineList = []string{"http://" + backupConf.Service}

	if backupConf.RecoveryTime <= 0 && id == "" {
		er(`Parameter '--id' and 'recovery_timestamp' cannot both be empty.
		You can use parameter '--id' to recover all backup with this backupID.
		Or you can recover backup from the Unix timestamp 'recovery_timestamp'.
		Full recovery is prefered when both are given.`)
	}

	recoveryRequest.Full = BackupInfo{
		InstanceID:  backupConf.InstanceID,
		BackupID:    id,
		BackupJobID: BACKUP_JOB_ID,
	}

	recoveryRequest.Incremental = BackupInfo{
		InstanceID:  backupConf.InstanceID,
		BackupID:    DEFAULT_INCREMENTAL_BACKUPID,
		BackupJobID: BACKUP_JOB_ID,
	}

	if id == "" {
		recoveryRequest.RecoveryTime = backupConf.RecoveryTime
	}

	recoveryRequest.BackupPBD = backupConf.RecoverPBD

	recoveryRequest.BackupStorageSpace = backupConf.BackupStorageSpace

	recoveryRequest.BackupJobID = BACKUP_JOB_ID
	recoveryRequest.Filesystem = BACKUP_FILESYSTEM
	recoveryRequest.BackupMetaSource = BACKUP_META_SOURCE

	recoveryRequest.WorkerCount = backupConf.WorkerCount

	recoveryRequest.EnableEncryption = backupConf.EnableEncryption

	recoveryRequest.EncryptionPassword = backupConf.EncryptionPassword

	recoveryRequest.UseBlock = true

	InitService(backupConf.Service)
	_, err := ServiceRecoverBackup(recoveryRequest)
	if err != nil {
		er(err.Error())
	}

	if id != "" {
		fmt.Printf("request recover from %s success, full backup id is: %s\n", backupConf.InstanceID, recoveryRequest.Full.BackupID)
	} else if backupConf.RecoveryTime != 0 {
		fmt.Printf("request recover from %s success, pitr time is: %d\n", backupConf.InstanceID, recoveryRequest.RecoveryTime)
	}

}

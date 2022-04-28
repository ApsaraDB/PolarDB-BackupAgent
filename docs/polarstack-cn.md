# PolarDB共享存储形态开源备份工具使用指南

## 安装方法
### 安装程序
- 首次安装程序，执行以下命令，安装PolarDB Stack开源备份工具。这里注意将xxxx替换为实际的版本号。
当前安装程序：t-polardb-backupagent-x.x.x-xxx.alios7.x86_64.rpm
```bash
sudo rpm -i t-polardb-backupagent-1.0.0-20220430120000.alios7.x86_64.rpm
```
### 升级程序
- 执行以下命令，升级PolarDB Stack开源备份工具，升级的时候需保证备份工具没在执行备份或者还原操作。这里注意将xxxx替换为实际的版本号。
```bash
sudo rpm -U --nodeps t-polardb-backupagent-1.0.0-20220430120000.alios7.x86_64.rpm
```
### 卸载程序
- 执行以下命令卸载程序，rpm -e卸载已将必要的文件和数据删除，无需执行额外的清理动作。这里要注意，卸载的时候需保证备份工具没在执行备份或者还原操作。
```bash
sudo rpm -e t-polardb-backupagent
```

## 使用方法
### 配置日志路径
- 打开配置文件，/usr/local/polardb_o_backup_tool_current/bin/loader.conf，修改字段"Logdir"，如设置日志输出目录为/home/polardb/log
```bash
"Logdir": "/home/polardb/log"
```
### 执行备份配置
- 参考/usr/local/polardb_o_backup_tool_current/bin/sample.backup.conf，渲染备份配置文件
```bash
{
    "Service": "127.0.0.1:1888",
    "InstanceID": "cluster-sample-bkt",
    "Filesystem": "pfs.file",
    "BackupPBD": "mapper_32ze2ahofeot2p504rm5r",
    "RecoverPBD": "mapper_32ze2ahofeot2p504rm5r",
    "RecoveryTime": 1650269966,
    "Compress": true,
    "WorkerCount": 16,
    "MaxBackupSpeed": 0,
    "EnableEncryption": false,
    "EncryptionPassword": "123456",
    "BackupAccount": {"User": "replicator","Password": "xxxxxxxxxxxxxxxxxxxx","Endpoint": "xx.xx.xx.xx","Port": "5797"},
    "BackupStorageSpace": {"StorageType":"fs", "Locations":{"Local": {"Path": "/disk1/testbackup"}}}
}
```

| 备份参数 | 含义 | 默认值 | 说明 |
| ------ | ---- | ---- | ---- |
| Service | 服务endpoint | 127.0.0.1:1888 | 备份agent服务端口默认为1888，服务endpoint为ip+":1888" |
| InstanceID | 备份数据库实例名 | 无 | instanceid为数据库的实例id如polar-rwo-2l5i0d93g33 |
| Filesystem | pg所使用的文件系统 | pfs.file | polarstack共享存储默认为pfs.file，无需修改 |
| BackupPBD | pg所使用的pfs存储设备，备份时使用 | 无 | pg所使用的存储设置，前缀一般为mapper_，后缀可通过ls /dev/mapper找到 |
| RecoverPBD | pg所使用的pfs存储设备，恢复时使用 | 无 | pg所使用的存储设置，恢复时使用，备份时无需填写该参数 |
| RecoveryTime | 恢复到指定时间点 | 无 | pg恢复到的指定时间点，unix格式，如1650269966，恢复到指定备份集填0即可，备份时无需填写该参数 |
| Compress | 是否启动压缩 | true | 备份文件时是否启动压缩，默认启动，减少备份所需存储空间，提升备份速度 |
| WorkerCount | 并发数量 | 16 | 备份并发备份的参数，如16，注意，较大的参数会增加IO/CPU/内存的占用，用户需在备份速度和业务运行效率进行权衡，如在业务空闲时可适当提高并发，提高备份速度 |
| MaxBackupSpeed | 最大备份速度限制 | 0 | 限制最大备份速度，单位为MB/s，0为不限速 |
| EnableEncryption | 加密使能 | false | 配置备份文件是否加密，默认为false，如需开启加密功能，填true |
| EncryptionPassword | 加密密码 | 无 | 默认为空，如果开启加密则使用备份程序内置的密码进行备份加密，如果想自行设置则填写自定义密码即可，注意备份程序不会保存加密密码，用户需妥善保存密码，密码丢失备份集将不可恢复 |
| BackupAccount | 备份账号信息 | 无 | User为usperuser的用户名如polardb；Password为usperuser的密码经base64加密；Endpoint为主节点的ip；Port为主节点的端口 |
| BackupStorageSpace | 备份集存储信息，支持nfs/s3/http | 无 | nfs示例：{"StorageType":"fs", "Locations":{"Local": {"Path": "/disk1/testbackup"}}} s3示例：{"StorageType":"s3", "Locations":{"s3": {"Accesskey": "minioadmin", "Bucket": "backup", "Endpoint": "127.0.0.1:9000", "Region": "", "Secretkey": "minioadmin"}}} http示例：{"StorageType":"http", "Locations":{"http": {"Endpoint": "127.0.0.1:8080"}}} |

### 执行增量备份
- 执行增量备份命令
```bash
cd /usr/local/polardb_o_backup_tool_current/bin
./pbkcli --config sample.backup.conf start --mode incremental
```

- 成功启动增量备份后，backup.log里会打印以下日志
```bash
[INFO] increpipeline start xx workers to do backup
```

- 备份备份启动后会在后台一直运行，如需停止增量备份，可执行以下命令
```bash
cd /usr/local/polardb_o_backup_tool_current/bin
./pbkcli --config sample.backup.conf stop --mode incremental
```

### 执行全量备份
- 执行全量备份命令
```bash
cd /usr/local/polardb_o_backup_tool_current/bin
./pbkcli --config sample.backup.conf start --mode full
```

- 全量备份完成后， 会在中打印打出backup.log打印以下日志。
```bash
[INFO] main [pgpipeline] Run success
```

### 执行还原集群
- 还原PolarDBStack集群的总体流程是先创建一个新的集群，然后停下数据库和cm组件，通过恢复脚本恢复数据到pfs存储设备，然后拉起数据库，最后再恢复cm组件和更新数据库内部账号的密码
- kubectl apply -f your-cluster.yaml创建一个仅包含rw节点（不包含ro节点）的集群
- 停止cm组件。执行kubectl edit deployment <数据库集群id>.cm，<数据库集群id>为上一步骤新建的数据库集群的id如polar-rwo-xxxxxxxxxxxx
```bash
kubectl edit deployment polar-rwo-xxxxxxxxxxxx.cm
```
  将replicas字段由1改为0，并保存
```bash
replicas: 0
```
- 执行kubectl get pod -o wide | grep <数据库集群id> 找出集群的rw节点
```bash
kubectl get pod -o wide | grep polar-rwo-xxxxxxxxxxxx
# 如这里找出rw节点 polar-rwo-xxxxxxxxxxxx-20386-20387
```
- 进入rw节点，data目录下生成ins_lock文件，并停止数据库
```bash
kubectl exec -it polar-rwo-xxxxxxxxxxxx-20386-20387 -c engine bash
touch /data/ins_lock
pg_ctl stop -D /data
```
- 初始化新集群的pfs设备
```bash
pfs -C disk mkfs -u 30 -l 1073741824 -f mapper_pv-36e00084100ee7ec97263216b00001f17
```
- 参考4.2小节，配置sample.backup.conf的以下三个字段
```bash
"Service": "127.0.0.1:1888",
"RecoverPBD": "mapper_32ze2ahofeot2p504rm5r",
"RecoveryTime": 1650269966,
```
- 执行恢复命令
```bash
cd /usr/local/polardb_o_backup_tool_current/bin
# 全量备份集还原示例
./pbkcli --config sample.backup.conf recover --id Full-20220415122703
# 按时间点还原示例
./pbkcli --config sample.backup.conf recover
```
  还原结束后backup.log会打印以下日志
```bash
# 全量备份集还原最后打印以下信息
main [pgpipeline.restore] Run success

# 按时间点还原最后打印以下信息
main [increpipeline.restore] Run success
```

- 进入rw，更新pg_control文件，更新后可执行命令pg_controldata /data查看更新后的信息
```bash
# rw节点更新pg_control文件示例
kubectl exec -it polar-rwo-xxxxxxxxxxxx-20386-20387 -c engine bash
su postgres
rm /data/global/pg_control
pfs -C disk read /mapper_pv-36e00084100ee7ec97263216b00001f17/data/global/pg_control >> /data/global/pg_control
```
- 进入rw节点，执行以下命令生成backup_label
```bash
su postgres
sudo pfs -C disk read /mapper_pv-36e00084100ee7ec97263216b00001f17/data/polar_exclusive_backup_label >> /data/backup_label
```
- 如恢复全量备份集，可忽略本步骤。在$PGDATA目录下以postgres用户创建recovery.conf，并添加还原时间点，如期望还原到时间：2020-12-26 16:01:40
```bash
touch /data/recovery.conf
echo "recovery_target_action='promote'" >> $PGDATA/recovery.conf
echo "recovery_target_time = '2020-12-26 16:01:40'" >> $PGDATA/recovery.conf
echo "restore_command = 'echo not copy'" >> $PGDATA/recovery.conf
```
- 进入备份工具执行文件目录，切换数据库用户如polardb，执行以下脚本（注意：如数据在k8s或者docker中拉起，与备份程序不在一个环境内，则将脚本拷贝到k8s或者docker中去执行）
```bash
./restore-initdb.sh
```
- 进入rw节点，启动数据库
```bash
kubectl exec -it polar-rwo-xxxxxxxxxxxx-20386-20387 -c engine bash
pg_ctl start -D /data
```
- 等待数据库恢复数据完成，执行重启，并删除data目录下生成的ins_lock文件
```bash
pg_ctl restart -D /data
rm /data/ins_lock
```
- 执行以下命令清理可能存在的inactive slots
```bash
select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_type='physical' and active='f';
```
- 获取新集群aurora以及replicator的密码，以新集群名为polar-rwo-xxxxxxxxxxxx为例
  搜索aurora和replicator密码相关的yaml信息
```bash
kubectl get Secret |grep polar-rwo-xxxxxxxxxxxx
# 显示如下
polar-rwo-xxxxxxxxxxxx-20906-aurora          Opaque                      3      10h
polar-rwo-xxxxxxxxxxxx-20906-replicator      Opaque                      3      10h
```
  获取aurora base64加密后的密码
```bash
kubectl get Secret polar-rwo-xxxxxxxxxxxx-20906-aurora -o yaml
# 显示如下
apiVersion: v1
data:
  Account: YXVyb3Jh
  Password: xxxxxxxxxxxxxxxxxxxx
```
  获取aurora原始密码
```bash
echo 'xxxxxxxxxxxxxxxxxxxx' | base64 --decode
# 显示如下
xxxxxxxxxxxxxxx
```
  获取replicator base64加密后的密码
```bash
kubectl get Secret polar-rwo-xxxxxxxxxxxx-20906-replicator -o yaml
# 显示如下
apiVersion: v1
data:
  Account: cmVwbGljYXRvcg==
  Password: xxxxxxxxxxxxxxxxxxxx
```
  获取replicator原始密码
```bash
echo 'xxxxxxxxxxxxxxxxxxxx' | base64 --decode
# 显示如下
xxxxxxxxxxxxxxx
```
- 登陆数据库集群rw节点，更新aurora以及replicator的密码
  登陆rw节点
```bash
kubectl exec -it polar-rwo-xxxxxxxxxxxx-20907-20908 bash -c engine
```
  登陆数据库并修改aurora以及replicator的密码
```bash
postgres=# ALTER USER aurora WITH PASSWORD 'xxxxxxxxxxxxxxx';
ALTER ROLE
postgres=# ALTER USER replicator WITH PASSWORD 'xxxxxxxxxxxxxxx';
ALTER ROLE
```
- 启动cm组件。执行kubectl edit deployment <数据库集群id>.cm，<数据库集群id>为上一步骤新建的数据库集群的id如polar-rwo-xxxxxxxxxxxx
```bash
kubectl edit deployment polar-rwo-xxxxxxxxxxxx.cm
```
  将replicas字段由0改为1，并保存
```bash
replicas: 1
```
## 运维指导
### 常见问题
- 备份恢复，启动数据库时报错：logical replication slot "xxxx" exists, but wal_level < logical, change wal_level to be logical or higher
  备份的数据库的wal_level设置成logical，因此恢复的时候也要设置该参数为logical或者更高的等级
### 查看日志
每条日志会通过前缀标志其等级，当前日志可分为四个等级
| 日志等级 | 日志说明 |
| ------- | ------ |
| INFO | INFO为正常显示的信息，如备份的备份集信息、状态、进度、耗时等 |
| DEBUG | DEBUG为开发调试使用，一般在用户使用时不会出现 |
| WARNING | 警告信息，运维人员应定期排查分析WARNING出现的原因 |
| ERROR | 错误信息，说明备份过程出错，运维人员应及时排查分析ERROR出现的原因 |
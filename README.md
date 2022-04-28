# PolarDB-BackupAgent
A distributed, high performance and high avaliable backup agent for polardb pg which has rich features and is easy to extend storage plugins.

# Compile dependence
- [compile and install pfs](https://github.com/ApsaraDB/PolarDB-FileSystem)
- After compile pfs, copy libpfs.a to /usr/local/polarstore/pfsd/lib
# How to build and start the backup service:
1. Enter root directory
2. make all
3. cd run
4. ./backup_ctl -config /path/to/run

# How to build rpm
1. Enter root directory
2. make rpm

# distributed
## shared disk
You can refer to [polarstack](docs/polarstack-cn.md) for using introduction in polarstack shared disk enviroment.
## shared nothing
TODO
# single
TODO
# Software License
PolarDB-BackupAgent is developed based on[ the open source software license Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
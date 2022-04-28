#ifndef __UE_PG_WALDUMP
#define __UE_PG_WALDUMP

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>

typedef unsigned long uint64;

typedef struct _WalInfo {
    uint64 starttime;
    uint64 endtime;
    uint64 startlsn;
    uint64 endlsn;
    uint64 prevlsn;
}WallInfo;

int GetWalInfo(const char *walname, int walsize, int pfs, WallInfo *info, const char *blockBackupDir, bool enable_blockbackup);
int InitWalSize(const char *name, int pfs);
#ifdef __cplusplus
}
#endif
#endif

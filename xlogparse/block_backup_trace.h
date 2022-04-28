#include <sys/file.h>
#include <unistd.h>

#include "commands/dbcommands_xlog.h"
#include "catalog/storage_xlog.h"

#include "datapagemap.h"
#include "hash_map.h"

typedef struct pgFile
{
    char   *rel_path;
    const char   *block_backup_datadir;
    datapagemap_t	pagemap;
}pgFile;

typedef void (*SyncLocalCB) (map_t bfm);
typedef void (*SyncBlockCB) (map_t bfm, char* rel_path, int blkno_inseg, const char* block_backup_datadir);

typedef struct BlockBackupTracer
{
    map_t backup_file_map;
    const char   *block_backup_datadir;

    SyncLocalCB syncLocal;
    SyncBlockCB syncBlock;
}BlockBackupTracer;

void extractRecordInfo(XLogReaderState *record, BlockBackupTracer* bbt);
void process_block_change(ForkNumber forknum, RelFileNode rnode, BlockNumber blkno, BlockBackupTracer* bbt);
void syncLocal(map_t bfm);
void syncBlock(map_t bfm, char* rel_path, int blkno_inseg, const char *block_backup_datadir);
void initBlockBackupTracer(BlockBackupTracer* bbt, const char *block_backup_datadir);
void freeBlockBackupTracer(BlockBackupTracer* bbt);
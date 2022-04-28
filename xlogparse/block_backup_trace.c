#include "postgres_fe.h"
#include "block_backup_trace.h"

// char		*block_backup_datadir = "/tmp";

void 
initBlockBackupTracer(BlockBackupTracer* bbt, const char* block_backup_datadir)
{
    bbt->syncLocal = syncLocal;
    bbt->syncBlock = syncBlock;

    bbt->backup_file_map = hashmap_new();
	bbt->block_backup_datadir = block_backup_datadir;
}

void
freeBlockBackupTracer(BlockBackupTracer* bbt)
{
	if(bbt->backup_file_map != NULL) {
		hashmap_free(bbt->backup_file_map);
		bbt->backup_file_map = NULL;
	}
}

void
syncBlock(map_t bfm, char* rel_path, int blkno_inseg, const char* block_backup_datadir)
{
    int error;
    pgFile* file_item;

    error = hashmap_get(bfm, rel_path, (void**)(&file_item));
    if(error == MAP_OK){
        // get file
    } else if (error == MAP_MISSING) {
        // create file
        file_item = malloc(sizeof(pgFile));
		MemSet(file_item, 0, sizeof(pgFile));
        file_item->rel_path = rel_path;
		file_item->block_backup_datadir = block_backup_datadir;
        error = hashmap_put(bfm, rel_path, file_item);
		if(error != MAP_OK) {
			printf("[ERROR] hashmap_put error");
		}
    } else {
        // error
		printf("[ERROR] invalid error code from hashmap_get: %d", error);
    }

    datapagemap_add(&(*file_item).pagemap, blkno_inseg);
}

int iterateSync(any_t item, any_t data){
	pgFile* file_item;
	int lock_fd;
	int ret;
	int i;

	file_item = (pgFile*)data;

	int l = strlen(file_item->rel_path);
	char* format_path = malloc(l + 1);
	for(i = 0; i <= l; i++) {
		if(file_item->rel_path[i] == '\0'){
			format_path[i] = '\0';
			break;
		} else if (file_item->rel_path[i] == '/'){
			format_path[i] = '-';
		} else {
			format_path[i] = file_item->rel_path[i];
		}
	}

	// lock
	int extra_len = 10;
	char* lock_name = malloc(strlen(file_item->block_backup_datadir) + strlen(format_path) + extra_len);
	sprintf(lock_name, "%s/%s.lock", file_item->block_backup_datadir, format_path);

	printf("[INFO] iterate sync for %s\n", file_item->rel_path);

	lock_fd = open(lock_name , O_RDONLY | O_CREAT , 0664);
	if(lock_fd < 0){
		printf("[ERROR] open %s error\n", lock_name);
		free(format_path);
		free(lock_name);
		return MAP_OK;
	}

	ret = flock(lock_fd , LOCK_EX);
	if(ret < 0){
		printf("[ERROR] flock %d error\n", lock_fd);
		free(format_path);
		free(lock_name);
		close(lock_fd);
		return MAP_OK;
	}

    // load local if exist
	char* file_name = malloc(strlen(file_item->block_backup_datadir) + strlen(format_path) + extra_len);
	sprintf(file_name, "%s/%s", file_item->block_backup_datadir, format_path);

	if((access(file_name, F_OK)) != -1)   
    {   
		printf("[INFO] found %s in local, do merging ...\n", file_name);

		FILE* pFile = fopen(file_name, "rb");
		if (pFile == NULL)
		{
			printf("[ERROR] fopen %s error\n", file_name);
			free(file_name);
			free(format_path);
			free(lock_name);
			close(lock_fd);
			return MAP_OK;
		}
	
		fseek(pFile , 0 , SEEK_END);
		long lSize = ftell (pFile);
		rewind(pFile);
	
		char* buffer = (char*) malloc(sizeof(char) * lSize);
		if (buffer == NULL)
		{
			printf("[ERROR] malloc error\n");
			free(file_name);
			free(format_path);
			free(lock_name);
			free(buffer);
			close(lock_fd);
			return MAP_OK;
		}

		long result = fread(buffer, 1, lSize, pFile);
		if (result != lSize)
		{
			printf("[ERROR] fread %s error\n", file_name);
		}

		datapagemap_t* local_map = malloc(sizeof(datapagemap_t));
		local_map->bitmap = buffer;
		local_map->bitmapsize = (int)lSize;

		datapagemap_iterator_t* it = datapagemap_iterate(local_map);
		int blk_no;
		while(datapagemap_next(it, &blk_no)) {
			datapagemap_add(&file_item->pagemap, blk_no);
		}

		printf("[INFO] merge %s done\n", file_name);

		fclose(pFile);
		free(buffer);
		free(local_map);
    }   
  
	int fd = open(file_name, O_CREAT | O_WRONLY | O_FSYNC, 0666);
	write(fd, file_item->pagemap.bitmap, file_item->pagemap.bitmapsize);
	close(fd);
	printf("[INFO] sync %s success, bitsize:%d\n", file_name, file_item->pagemap.bitmapsize);

    // update local

    // unlock
	ret = flock(lock_fd , LOCK_UN);
	if(ret < 0){
		printf("[ERROR] unflock %d error\n", lock_fd);
	}

	// free & close
	free(format_path);
	free(lock_name);
	free(file_name);
	close(lock_fd);

    return MAP_OK;
}

void
syncLocal(map_t bfm)
{
    // iterate map
	PFany IterateFunc = iterateSync;
    hashmap_iterate(bfm, IterateFunc, NULL);
}

void
extractRecordInfo(XLogReaderState *record, BlockBackupTracer* bbt)
{
	uint8		block_id;
	RmgrId		rmid = XLogRecGetRmid(record);
	uint8		info = XLogRecGetInfo(record);
	uint8		rminfo = info & ~XLR_INFO_MASK;

	/* Is this a special record type that I recognize? */

	if (rmid == RM_DBASE_ID && rminfo == XLOG_DBASE_CREATE)
	{
		/*
		 * New databases can be safely ignored. They would be completely
		 * copied if found.
		 */
	}
	else if (rmid == RM_DBASE_ID && rminfo == XLOG_DBASE_DROP)
	{
		/*
		 * An existing database was dropped. It is fine to ignore that
		 * they will be removed appropriately.
		 */
	}
	else if (rmid == RM_SMGR_ID && rminfo == XLOG_SMGR_CREATE)
	{
		/*
		 * We can safely ignore these. The file will be removed when
		 * combining the backups in the case of differential on.
		 */
	}
	else if (rmid == RM_SMGR_ID && rminfo == XLOG_SMGR_TRUNCATE)
	{
		/*
		 * We can safely ignore these. When we compare the sizes later on,
		 * we'll notice that they differ, and copy the missing tail from
		 * source system.
		 */
	}
	else if (info & XLR_SPECIAL_REL_UPDATE)
	{
		/*
		 * This record type modifies a relation file in some special way, but
		 * we don't recognize the type. That's bad - we don't know how to
		 * track that change.
		 */
		// elog(ERROR, "WAL record modifies a relation, but record type is not recognized\n"
		// 	 "lsn: %X/%X, rmgr: todo, info: %02X",
		//   (uint32) (record->ReadRecPtr >> 32), (uint32) (record->ReadRecPtr), info);
	}

	for (block_id = 0; block_id <= record->max_block_id; block_id++)
	{
		RelFileNode rnode;
		ForkNumber	forknum;
		BlockNumber blkno;

		if (!XLogRecGetBlockTag(record, block_id, &rnode, &forknum, &blkno))
			continue;

		/* We only care about the main fork; others are copied as is */
		if (forknum != MAIN_FORKNUM)
			continue;

		process_block_change(forknum, rnode, blkno, bbt);
	}
}

void
process_block_change(ForkNumber forknum, RelFileNode rnode, BlockNumber blkno, BlockBackupTracer* bbt)
{
//	char	   *path;
	char	   *rel_path;
	char	   *new_rel_path;
	BlockNumber blkno_inseg;
	int			segno;
	// pgFile	  **file_item;
	// pgFile		f;

	segno = blkno / RELSEG_SIZE;
	blkno_inseg = blkno % RELSEG_SIZE;

	new_rel_path = malloc(100 * sizeof(char));

	rel_path = relpathperm(rnode, forknum);
	if (segno > 0)
        // printf("rel: %s.%d, blkno_in: %d\n", rel_path, segno, blkno_inseg);
		sprintf(new_rel_path, "%s.%d", rel_path, segno);
	else
        // printf("rel: %s, blkno_in: %d\n", rel_path, blkno_inseg);
		sprintf(new_rel_path, "%s", rel_path);

    bbt->syncBlock(bbt->backup_file_map, new_rel_path, blkno_inseg, bbt->block_backup_datadir);

	free(rel_path);
	// f.external_dir_num = 0;
}
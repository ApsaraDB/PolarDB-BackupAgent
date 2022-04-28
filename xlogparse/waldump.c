#include "postgres.h"
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "access/xlog_internal.h"
#include "access/transam.h"
#include "access/xact.h"
#include "common/fe_memutils.h"
#include "getopt_long.h"
#include "catalog/pg_control.h"

#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <pfs_api.h>

#include "waldump.h"

#include "block_backup_trace.h"

#define ALIGN(x, mask)   ((x+mask-1) & (~ (mask-1)))

typedef int (*open_fptr)(const char *name, int, mode_t);
typedef int (*close_fptr)(int);
typedef ssize_t (*read_fptr)(int, void *, size_t);
typedef off_t (*lseek_fptr)(int, off_t, int);

typedef struct XLogDumpPrivate
{
	TimeLineID	timeline;
	const char  *fname;
	XLogRecPtr	startptr;
	XLogRecPtr	endptr;
	bool		endptr_reached;
    int         walsize;
    int         sendFile;
    int         sendSegNo;
    uint32      sendOff;
    read_fptr   read;
    lseek_fptr  lseek;
    open_fptr   open;
    close_fptr  close;
} XLogDumpPrivate;

static int
getRecordTimestamp(XLogReaderState *record, TimestampTz *recordXtime)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	uint8		xact_info = info & XLOG_XACT_OPMASK;
	uint8		rmid = XLogRecGetRmid(record);

	if (rmid == RM_XLOG_ID && info == XLOG_RESTORE_POINT)
	{
		*recordXtime = ((xl_restore_point *) XLogRecGetData(record))->rp_time;
		return 1;
	}
	if (rmid == RM_XACT_ID && (xact_info == XLOG_XACT_COMMIT ||
							   xact_info == XLOG_XACT_COMMIT_PREPARED))
	{
		*recordXtime = ((xl_xact_commit *) XLogRecGetData(record))->xact_time;
		return 1;
	}
	if (rmid == RM_XACT_ID && (xact_info == XLOG_XACT_ABORT ||
							   xact_info == XLOG_XACT_ABORT_PREPARED))
	{
		*recordXtime = ((xl_xact_abort *) XLogRecGetData(record))->xact_time;
		return 1;
	}
	return 0;
}

static void
XLogDumpXLogRead(XLogDumpPrivate *private, TimeLineID timeline_id,
				 XLogRecPtr startptr, char *buf, Size count)
{
	char	   *p;
	XLogRecPtr	recptr;
	Size		nbytes;

	p = buf;
	recptr = startptr;
	nbytes = count;

	while (nbytes > 0)
	{
		uint32		startoff;
		int			segbytes;
		int			readbytes;

		startoff = XLogSegmentOffset(recptr, private->walsize);

		if (private->sendFile < 0 || !XLByteInSeg(recptr, private->sendSegNo, private->walsize))
		{
			/* Switch to another logfile segment */
			if (private->sendFile >= 0) {
				private->close(private->sendFile);
                private->sendFile = -1;
            }

			XLByteToSeg(recptr, private->sendSegNo, private->walsize);
            private->sendFile = private->open(private->fname, O_RDONLY, 0);

			if (private->sendFile < 0)
                goto clean;
			private->sendOff = 0;
		}

		/* Need to seek in the file? */
		if (private->sendOff != startoff)
		{
			if (private->lseek(private->sendFile, (off_t) startoff, SEEK_SET) < 0)
                goto clean;
			private->sendOff = startoff;
		}

		/* How many bytes are within this segment? */
		if (nbytes > (private->walsize - startoff))
			segbytes = private->walsize - startoff;
		else
			segbytes = nbytes;

		readbytes = private->read(private->sendFile, p, segbytes);
		if (readbytes <= 0)
            goto clean;

		recptr += readbytes;
		private->sendOff += readbytes;
		nbytes -= readbytes;
		p += readbytes;
	}
clean:
    return;
}

static int
XLogDumpReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
				 XLogRecPtr targetPtr, char *readBuff, TimeLineID *curFileTLI)
{
	XLogDumpPrivate *private = state->private_data;
	int			count = XLOG_BLCKSZ;

	if (private->endptr != InvalidXLogRecPtr)
	{
		if (targetPagePtr + XLOG_BLCKSZ <= private->endptr)
			count = XLOG_BLCKSZ;
		else if (targetPagePtr + reqLen <= private->endptr)
			count = private->endptr - targetPagePtr;
		else
		{
			private->endptr_reached = 1;
			return -1;
		}
	}

	XLogDumpXLogRead(private, private->timeline, targetPagePtr,
					 readBuff, count);

	return count;
}

static int  _GetWalInfo(const char *walname, XLogDumpPrivate *private, WallInfo *info, const char *blockBackupDir, bool enable_blockbackup)
{
    XLogSegNo	segno;
	XLogReaderState *xlogreader_state;
	XLogRecPtr	first_record;
	XLogRecord *record;
	char	   *errormsg;
    TimestampTz xtime = 0;
    unsigned long ts = 0;
    int ret = -1;
    BlockBackupTracer blockBackupTracer;

    if (enable_blockbackup) {
        initBlockBackupTracer(&blockBackupTracer, blockBackupDir);
    }

    const char *basename;
    basename = strrchr(walname, '/');
    if (basename && basename[1] != '\0') {
        basename = &basename[1];
    } else {
        basename = walname;
    }

    XLogFromFileName(basename, &private->timeline, &segno, private->walsize);
    if (XLogRecPtrIsInvalid(private->startptr)) {
        XLogSegNoOffsetToRecPtr(segno, 0, private->walsize, private->startptr);
    } else if (!XLByteInSeg(private->startptr, segno, private->walsize)) {
        return ret;
    }
    XLogSegNoOffsetToRecPtr(segno + 1, 0, private->walsize, private->endptr);
	if (XLogRecPtrIsInvalid(private->startptr)) {
        return ret;
    }
	xlogreader_state = XLogReaderAllocate(private->walsize, XLogDumpReadPage, private);
	if (!xlogreader_state) {
        return ret;
    }
	first_record = XLogFindNextRecord(xlogreader_state, private->startptr);
	if (first_record == InvalidXLogRecPtr) {
        goto clean;
    }
    int first_time = true;
    int first_lsn = true;
    for (;;) {
		record = XLogReadRecord(xlogreader_state, first_record, &errormsg);
        if (!record) {
            goto clean;
        }

        if (enable_blockbackup) {
            extractRecordInfo(xlogreader_state, &blockBackupTracer);
        }

		first_record = InvalidXLogRecPtr;
        if (first_lsn) {
            info->startlsn = xlogreader_state->currRecPtr;
            first_lsn = false;
        }
        info->endlsn = xlogreader_state->currRecPtr;
        info->prevlsn = XLogRecGetPrev(xlogreader_state);
        if (getRecordTimestamp(xlogreader_state, &xtime)) {
            ts = (xtime / 1000000 + (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);
            if (first_time) {
                first_time = false;
                ret = 0;
                info->starttime = ts;

            }
            info->endtime = ts;
        }
    }
clean:
	XLogReaderFree(xlogreader_state);

	struct timeval starttime,endtime;
    gettimeofday(&starttime,0);

    if (enable_blockbackup) {
        blockBackupTracer.syncLocal(blockBackupTracer.backup_file_map);
        freeBlockBackupTracer(&blockBackupTracer);
    }

	gettimeofday(&endtime,0);
	double timeuse = 1000000 * (endtime.tv_sec - starttime.tv_sec) + endtime.tv_usec - starttime.tv_usec;
	timeuse /= 1000;
	printf("[INFO] sync local time use: %f ms\n", timeuse);

    return ret;
}

static void XLogDumpPrivateInit(XLogDumpPrivate *private, open_fptr f_open, close_fptr f_close, lseek_fptr f_lseek, read_fptr f_read) {
    private->open = f_open;
    private->close = f_close;

    private->lseek = f_lseek;
    private->read = f_read;
}

static int native_open(const char *name, int flags, mode_t mode) {
    return open(name, flags, mode);

}

static int native_close(int fd) {
    return close(fd);
}


static off_t native_lseek(int fd, off_t offset, int whence) {
    return lseek(fd, offset, whence);
}

static ssize_t native_read(int fd, void *buf, size_t size) {

    return read(fd, buf, size);
}

int InitWalSize(const char *name, int pfs) {
    int fd;
    int ret = 0;

    PGAlignedXLogBlock buf;
    XLogDumpPrivate private;
    memset(&private, 0, sizeof(XLogDumpPrivate));

    if (pfs) {
        XLogDumpPrivateInit(&private, pfs_open, pfs_close, pfs_lseek, pfs_read);

    } else {
        XLogDumpPrivateInit(&private, native_open, native_close, native_lseek, native_read);
    }

    fd = private.open(name, O_RDONLY, 0);
    if (fd < 0) {

        return ret;
    }
    if (private.read(fd, buf.data, XLOG_BLCKSZ) == XLOG_BLCKSZ) {
        XLogLongPageHeader longhdr = (XLogLongPageHeader) buf.data;
        if (IsValidWalSegSize(longhdr->xlp_seg_size)) {
            ret = longhdr->xlp_seg_size;

        }
    }
    private.close(fd);
    return ret;
}


int GetWalInfo(const char *walname, int walsize, int pfs, WallInfo *info, const char *blockBackupDir, bool enable_blockbackup)
{
    XLogDumpPrivate private;
    int ret;
    memset(&private, 0, sizeof(XLogDumpPrivate));
    private.fname = walname;
    if (pfs) {
        XLogDumpPrivateInit(&private, pfs_open, pfs_close, pfs_lseek, pfs_read);

    } else {
        XLogDumpPrivateInit(&private, native_open, native_close, native_lseek, native_read);
    }
    private.sendFile = -1;
    private.walsize = walsize;
    ret = _GetWalInfo(walname, &private, info, blockBackupDir, enable_blockbackup);
    if (private.sendFile >= 0) {
        private.close(private.sendFile);
    }
    return ret;
}


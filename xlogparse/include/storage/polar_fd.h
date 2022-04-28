/*-------------------------------------------------------------------------
 *
 * polar_fd.h
 *	  Polardb Virtual file descriptor definitions.
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * IDENTIFICATION
 *      src/include/storage/polar_fd.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef POLARFD_H
#define POLARFD_H

#include <dirent.h>
#include <sys/stat.h>
#include "utils/resowner.h"


/*
 * polar file system interface
 */
#define POLAR_FILE_IN_SHARED_STORAGE()				(polar_enable_shared_storage_mode)

#define POLAR_ENABLE_PWRITE()			(polar_enable_pwrite)
#define POLAR_ENABLE_PREAD()			(polar_enable_pread)

#define	AmPolarBackgroundWriterProcess()		(polar_enable_shared_storage_mode && AmBackgroundWriterProcess())

#define	POLAR_VFS_SWITCH_LOCAL		0
#define	POLAR_VFS_SWITCH_PLUGIN		1

#define POLAR_VFS_UNKNOWN_FILE			-1
#define POLAR_VFS_PROTOCOL_MAX_LEN		64
#define POLAR_VFS_PROTOCOL_TAG			"://"
#define POLAR_VFS_PROTOCAL_LOCAL_BIO	"file://"
#define POLAR_VFS_PROTOCAL_PFS			"pfsd://"
#define POLAR_VFS_PROTOCAL_LOCAL_DIO	"file-dio://"

#define POLAR_BUFFER_ALIGN_LEN			(4096)
#define POLAR_BUFFER_EXTEND_SIZE(LEN)	(LEN + POLAR_BUFFER_ALIGN_LEN)
#define POLAR_BUFFER_ALIGN(LEN)			TYPEALIGN(POLAR_BUFFER_ALIGN_LEN, LEN)

/*
 * ploar VFS interface kind 
 */
typedef enum PolarVFSKind{
	POLAR_VFS_LOCAL_BIO = 0,
	POLAR_VFS_PFS = 1,
	POLAR_VFS_LOCAL_DIO = 2,
	/* NB: Define the size here for future maintenance */
	POLAR_VFS_KIND_SIZE = 3
}PolarVFSKind;

typedef enum PolarNodeType
{
	POLAR_UNKNOWN = 0,
	POLAR_MASTER = 1,
	POLAR_REPLICA,
	POLAR_STANDBY,
	/* POLAR: datamax mode with independent storage, datamax mode with shared storage is not supported */
	POLAR_STANDALONE_DATAMAX
} PolarNodeType;

extern PolarNodeType	polar_local_node_type;
extern bool 	polar_mount_shared_storage_readonly_mode;
extern int	 	polar_vfs_switch;
extern bool		polar_openfile_with_readonly_in_replica;
extern char *polar_datadir;
extern bool polar_enable_shared_storage_mode;

typedef int	(*vfs_open_type)(const char *path, int flags, mode_t mode);

typedef struct vfs_mgr
{
	int (*vfs_env_init)(void);
	int (*vfs_env_destroy)(void);
	int (*vfs_mount)(void);
	int (*vfs_remount)(void);
	int (*vfs_open)(const char *path, int flags, mode_t mode);
	int (*vfs_creat)(const char *path, mode_t mode);
	int (*vfs_close)(int fd);
	ssize_t (*vfs_read)(int fd, void *buf, size_t len);
	ssize_t (*vfs_write)(int fd, const void *buf, size_t len);
	ssize_t (*vfs_pread)(int fd, void *buf, size_t len, off_t offset);
	ssize_t (*vfs_pwrite)(int fd, const void *buf, size_t len, off_t offset);
	int (*vfs_stat)(const char *path, struct stat *buf);
	int (*vfs_fstat)(int fd, struct stat *buf);
	int (*vfs_lstat)(const char *path, struct stat *buf);
	off_t (*vfs_lseek)(int fd, off_t offset, int whence);
	off_t (*vfs_lseek_cache)(int fd, off_t offset, int whence);
	int (*vfs_access)(const char *path, int mode);
	int (*vfs_fsync)(int fd);
	int (*vfs_unlink)(const char *path);
	int (*vfs_rename)(const char *oldpath, const char *newpath);
	int (*vfs_fallocate)(int fd, off_t offset, off_t len);
	int (*vfs_ftruncate)(int fd, off_t len);
	DIR *(*vfs_opendir)(const char *path);
	struct dirent *(*vfs_readdir)(DIR *dir);
	int (*vfs_closedir)(DIR *dir);
	int (*vfs_mkdir)(const char *path, mode_t mode);
	int (*vfs_rmdir)(const char *path);
	const struct vfs_mgr* (*vfs_mgr_func)(const char *path);
	int (*vfs_chmod) (const char *path, mode_t mode);
} vfs_mgr;

extern vfs_mgr polar_vfs[];

extern int polar_make_pg_directory(const char *directoryName);
extern ssize_t polar_read_line(int fd, void *buffer, size_t len);
extern void polar_copy_file(char *fromfile, char *tofile, bool skiperr);
extern void polar_copydir(char *fromdir, char *todir, bool recurse, bool clean, bool skip_file_err, 
						  bool skip_open_dir_err);
extern struct dirent * polar_read_dir_ext(DIR *dir, const char *dirname, int elevel, int *err);

extern void polar_register_tls_cleanup(void);
extern void polar_validate_dir(char *path);
extern void polar_init_node_type(void);
extern PolarNodeType polar_node_type_by_file(void);
extern void assign_polar_datadir(const char *newval, void *extra);

static inline int	
polar_env_init(void)
{
	if (polar_vfs[polar_vfs_switch].vfs_env_init)	
		return polar_vfs[polar_vfs_switch].vfs_env_init();

	return 0;
}

static inline int
polar_env_destroy(void)
{
	if (polar_vfs[polar_vfs_switch].vfs_env_destroy)
		return polar_vfs[polar_vfs_switch].vfs_env_destroy();

	return 0;
}

static inline int
polar_mount(void)
{
	int ret = 0;
	if (polar_vfs[polar_vfs_switch].vfs_mount)
		ret = polar_vfs[polar_vfs_switch].vfs_mount();
	return ret;
}

static inline int
polar_remount(void)
{
	int ret = 0;
	if (polar_vfs[polar_vfs_switch].vfs_remount)
		ret = polar_vfs[polar_vfs_switch].vfs_remount();
	return ret;
}

static inline int
polar_open(const char *path, int flags, mode_t mode)
{
	return polar_vfs[polar_vfs_switch].vfs_open(path, flags, mode);
}

static inline int
polar_creat(const char *path, mode_t mode)
{
	return polar_vfs[polar_vfs_switch].vfs_creat(path, mode);
}

static inline int
polar_close(int fd)
{
	return polar_vfs[polar_vfs_switch].vfs_close(fd);
}

static inline ssize_t
polar_read(int fd, void *buf, size_t len)
{
	return polar_vfs[polar_vfs_switch].vfs_read(fd, buf, len);
}

static inline ssize_t
polar_write(int fd, const void *buf, size_t len)
{
	return polar_vfs[polar_vfs_switch].vfs_write(fd, buf, len);
}

static inline ssize_t
polar_pread(int fd, void *buf, size_t len, off_t offset)
{
	return polar_vfs[polar_vfs_switch].vfs_pread(fd, buf, len, offset);
}

static inline ssize_t
polar_pwrite(int fd, const void *buf, size_t len, off_t offset)
{
	return polar_vfs[polar_vfs_switch].vfs_pwrite(fd, buf, len, offset);
}

/* POLAR: Replacing stat function, check all unlink when every merge code */
static inline int
polar_stat(const char *path, struct stat *buf)
{
	return polar_vfs[polar_vfs_switch].vfs_stat(path, buf);
}

static inline int
polar_fstat(int fd, struct stat *buf)
{
	return polar_vfs[polar_vfs_switch].vfs_fstat(fd, buf);
}

static inline int
polar_lstat(const char *path, struct stat *buf)
{
	return polar_vfs[polar_vfs_switch].vfs_lstat(path, buf);
}

static inline off_t
polar_lseek(int fd, off_t offset, int whence)
{
	return polar_vfs[polar_vfs_switch].vfs_lseek(fd, offset, whence);
}

static inline off_t
polar_lseek_cache(int fd, off_t offset, int whence)
{
	return polar_vfs[polar_vfs_switch].vfs_lseek_cache(fd, offset, whence);
}

static inline int
polar_access(const char *path, int mode)
{
	return polar_vfs[polar_vfs_switch].vfs_access(path, mode);
}

static inline int
polar_fsync(int fd)
{
	return polar_vfs[polar_vfs_switch].vfs_fsync(fd);
}

/* POLAR: Replacing unlink function, check all unlink when every merge code */
static inline int
polar_unlink(const char *fname)
{
	return polar_vfs[polar_vfs_switch].vfs_unlink(fname);
}

static inline int
polar_rename(const char *oldfile, const char *newfile)
{
	return polar_vfs[polar_vfs_switch].vfs_rename(oldfile, newfile);
}

static inline int
polar_fallocate(int fd, off_t offset, off_t len)
{
	return polar_vfs[polar_vfs_switch].vfs_fallocate(fd, offset, len);
}

static inline int
polar_ftruncate(int fd, off_t len)
{
	return polar_vfs[polar_vfs_switch].vfs_ftruncate(fd, len);
}

static inline DIR	*
polar_opendir(const char *path)
{
	return polar_vfs[polar_vfs_switch].vfs_opendir(path);
}

static inline struct dirent *
polar_readdir(DIR *dir)
{
	return polar_vfs[polar_vfs_switch].vfs_readdir(dir);
}

static inline int
polar_closedir(DIR *dir)
{
	return polar_vfs[polar_vfs_switch].vfs_closedir(dir);
}

static inline int
polar_mkdir(const char *path, mode_t mode)
{
	return polar_vfs[polar_vfs_switch].vfs_mkdir(path, mode);
}

/* POLAR: Replacing rmdir function, check all unlink when every merge code */
static inline int
polar_rmdir(const char *path)
{
	return polar_vfs[polar_vfs_switch].vfs_rmdir(path);
}

static inline int
polar_chmod(const char *path, mode_t mode)
{
	int rc = 0;
	if (polar_vfs[polar_vfs_switch].vfs_chmod)
		rc = polar_vfs[polar_vfs_switch].vfs_chmod(path, mode);
	return rc;
}

static inline const vfs_mgr*
polar_get_local_vfs_mgr(const char *path)
{
	return &polar_vfs[POLAR_VFS_SWITCH_LOCAL];
}

static inline bool
polar_file_exists(const char *path)
{
	struct stat st;
	return (polar_stat(path, &st) == 0) && S_ISREG(st.st_mode);
}

static inline void
polar_reset_vfs_switch(void)
{
	polar_vfs_switch = POLAR_VFS_SWITCH_LOCAL;
}

static inline void
polar_set_vfs_function_ready(void)
{
	polar_vfs_switch = POLAR_VFS_SWITCH_PLUGIN;
}

/*
 * POLAR: We provide this macro in order to remove protocol
 * from polar_datadir. The polar_datadir must conform to the format:
 * [protocol]://[path]
 *
 * Notice: The polar_datadir's protocol must be same as the polar_vfs_klind
 * inside polar_vfs.c. This macro should ONLY be used when you can't use polar_vfs
 * interface.
 */
static inline const char *
polar_path_remove_protocol(const char *path)
{
	const char *vfs_path = strstr(path, POLAR_VFS_PROTOCOL_TAG);
	if (vfs_path)
		return vfs_path + strlen(POLAR_VFS_PROTOCOL_TAG);
	else
		return path;
}

static inline void
polar_make_file_path_level3(char *path, const char *base, const char *file_path)
{
	if (POLAR_FILE_IN_SHARED_STORAGE())
		snprintf(path, MAXPGPATH, "%s/%s/%s", polar_datadir, base, file_path);
	else
		snprintf(path, MAXPGPATH, "%s/%s", base, file_path);

	return;
}

static inline void
polar_make_file_path_level2(char *path, const char *file_path)
{
	if (POLAR_FILE_IN_SHARED_STORAGE())
		snprintf(path, MAXPGPATH, "%s/%s", polar_datadir, file_path);
	else
		snprintf(path, MAXPGPATH, "%s", file_path);

	return;
}

static inline const vfs_mgr*
polar_vfs_mgr(const char *path)
{
	if (polar_vfs[polar_vfs_switch].vfs_mgr_func)
		return (polar_vfs[polar_vfs_switch].vfs_mgr_func)(path);
	return NULL;
}

#endif

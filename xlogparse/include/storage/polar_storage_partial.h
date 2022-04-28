/*-------------------------------------------------------------------------
 *
 * polar_storage_partial.h
 *	  Storage partial -- declarations related to the
 *	  internal implementation of toast partial storage strategy, not the public API
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
 *      src/include/storage/polar_storage_partial.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_STORAGE_PARTIAL_H
#define POLAR_STORAGE_PARTIAL_H

/* POLAR Ganos: Toast partial */
#define TOAST_PARTIAL_MAX_SIZE     0x3F  /* value is 63 */

/*
 * So storage 8bits looks like:
 *    1 0 * * * * * *
 * 1) 1                : 1bit is the tag of store partial
 * 2)   0              : 1bit reserved, zero default
 * 3)     * * * * * *  : 6bits is size of partial internal
 */ 
#define ATT_STORAGE_TYPE_IS_PARTIAL(c) \
	((c) & 0x80)

#define ATT_SET_STORAGE_TYPE_PARTIAL(c, len) \
	((c) = ((len) | 0x80))

#define ATT_GET_STORAGE_PARTIAL_SIZE(c) \
	((c) & 0x3F)

/*
 * So tag is the same as storage, 8bits looks like:
 *    1 0 * * * * * *
 * 1) 1                : 1bit is the tag of store partial
 * 2)   0              : 1bit reserved, zero default
 * 3)     * * * * * *  : 6bits is size of partial internal
 */ 
#define VARTAG_SET_ONDISK_PARTIAL(len) \
	((len) | 0x80)

#define VARTAG_IS_ONDISK_PARTIAL(tag)    ATT_STORAGE_TYPE_IS_PARTIAL((tag))

#define VARTAG_ONDISK_PARTIAL_INTERNAL_SIZE(tag)  ATT_GET_STORAGE_PARTIAL_SIZE((tag))



#endif	/* POLAR_STORAGE_PARTIAL_H */
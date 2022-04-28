/*-------------------------------------------------------------------------
 *
 * datapagemap.c
 *	  A data structure for keeping track of data pages that have changed.
 *
 * This is a fairly simple bitmap.
 *
 * Copyright (c) 2013-2019, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "datapagemap.h"

struct datapagemap_iterator
{
	datapagemap_t *map;
	BlockNumber nextblkno;
};

/*
 * Util functions
 */

void *
pg_realloc(void *ptr, size_t size)
{
	void	   *tmp;

	/* Avoid unportable behavior of realloc(NULL, 0) */
	if (ptr == NULL && size == 0)
		size = 1;
	tmp = realloc(ptr, size);
	if (!tmp)
	{
		fprintf(stderr, _("out of memory\n"));
		exit(EXIT_FAILURE);
	}
	return tmp;
}

static inline void *
pg_malloc_internal(size_t size, int flags)
{
	void	   *tmp;

	/* Avoid unportable behavior of malloc(0) */
	if (size == 0)
		size = 1;
	tmp = malloc(size);
	if (tmp == NULL)
	{
		if ((flags & MCXT_ALLOC_NO_OOM) == 0)
		{
			fprintf(stderr, _("out of memory\n"));
			exit(EXIT_FAILURE);
		}
		return NULL;
	}

	if ((flags & MCXT_ALLOC_ZERO) != 0)
		MemSet(tmp, 0, size);
	return tmp;
}

void *
pg_malloc(size_t size)
{
	return pg_malloc_internal(size, 0);
}

/*
 * Public functions
 */

/*
 * Add a block to the bitmap.
 */
void
datapagemap_add(datapagemap_t *map, BlockNumber blkno)
{
	int			offset;
	int			bitno;
	int			oldsize = map->bitmapsize;

	offset = blkno / 8;
	bitno = blkno % 8;

	/* enlarge or create bitmap if needed */
	if (oldsize <= offset)
	{
		int			newsize;

		/*
		 * The minimum to hold the new bit is offset + 1. But add some
		 * headroom, so that we don't need to repeatedly enlarge the bitmap in
		 * the common case that blocks are modified in order, from beginning
		 * of a relation to the end.
		 */
		newsize = (oldsize == 0) ? 16 : oldsize;
		while (newsize <= offset) {
			newsize <<= 1;
		}

		map->bitmap = pg_realloc(map->bitmap, newsize);

		/* zero out the newly allocated region */
		memset(&map->bitmap[oldsize], 0, newsize - oldsize);

		map->bitmapsize = newsize;
	}

	/* Set the bit */
	map->bitmap[offset] |= (1 << bitno);
}

/*
 * Start iterating through all entries in the page map.
 *
 * After datapagemap_iterate, call datapagemap_next to return the entries,
 * until it returns false. After you're done, use pg_free() to destroy the
 * iterator.
 */
datapagemap_iterator_t *
datapagemap_iterate(datapagemap_t *map)
{
	datapagemap_iterator_t *iter;

	iter = pg_malloc(sizeof(datapagemap_iterator_t));
	iter->map = map;
	iter->nextblkno = 0;

	return iter;
}

bool
datapagemap_next(datapagemap_iterator_t *iter, BlockNumber *blkno)
{
	datapagemap_t *map = iter->map;

	for (;;)
	{
		BlockNumber blk = iter->nextblkno;
		int			nextoff = blk / 8;
		int			bitno = blk % 8;

		if (nextoff >= map->bitmapsize)
			break;

		iter->nextblkno++;

		if (map->bitmap[nextoff] & (1 << bitno))
		{
			*blkno = blk;
			return true;
		}
	}

	/* no more set bits in this bitmap. */
	return false;
}


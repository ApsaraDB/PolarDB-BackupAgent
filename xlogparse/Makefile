PWD = $(shell pwd)
INC = -I/usr/local/polarstore/pfsd/include  -I$(PWD)/include
CFLAGS = $(INC) -g -O2 -c -fPIC
SRCS = pg_crc32c_sb8.c xlogreader.c relpath.c hash_map.c datapagemap.c block_backup_trace.c waldump.c
OBJS = $(SRCS:.c=.o)

libwaldump.a: $(OBJS)
	ar rcf $@ $^

%.o : %.c
	$(CC) $(CFLAGS) $< -o $@

clean:
	rm -f libwaldump.a *.o

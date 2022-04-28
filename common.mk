PWD=$(shell pwd)

INCDIR=/usr/local/polarstore/pfsd/include
LIBDIR=/usr/local/polarstore/pfsd/lib

CGO_CFLAGS = -I${INCDIR} -I$(PWD)/../../xlogparse
CGO_LDFLAGS = -L${LIBDIR} -L$(PWD)/../../xlogparse -Wl,-Bstatic -lwaldump -lpfs -Wl,-Bdynamic -lrt -ldl -laio

#FLAGS = -gcflags=all="-N -l" -ldflags="-compressdwarf=false -static"
FLAGS = -ldflags='-w'

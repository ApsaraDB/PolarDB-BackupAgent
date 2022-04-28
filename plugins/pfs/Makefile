include ../../common.mk

all : pfs.so
	cp *.so ../../run

clean:
	-rm -f *.so

pfs.so: pfs.go
	go env -w CGO_CFLAGS="${CGO_CFLAGS}"
	go env -w CGO_LDFLAGS="${CGO_LDFLAGS}"
	go build -o $@ $(FLAGS) -buildmode=plugin $^
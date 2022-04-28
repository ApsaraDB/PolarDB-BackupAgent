include ../../common.mk

all : minio.so
	cp *.so ../../run

minio.so: minio.go
	go build -o $@ $(FLAGS) -buildmode=plugin $^

clean:
	-rm -f *.so
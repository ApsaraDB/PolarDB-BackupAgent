include ../../common.mk

all : oss.so
	cp *.so ../../run

oss.so: oss.go
	go build -o $@ $(FLAGS) -buildmode=plugin $^

clean:
	-rm -f *.so
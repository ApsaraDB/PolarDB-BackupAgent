include ../../common.mk

all : fs.so
	cp *.so ../../run

fs.so: fs.go
	go build -o $@ $(FLAGS) -buildmode=plugin $^

clean:
	-rm -f *.so
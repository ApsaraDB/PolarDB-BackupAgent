include ../../common.mk

all : http.so
	cp *.so ../../run

http.so: http.go
	go build -o $@ $(FLAGS) -buildmode=plugin $^

clean:
	-rm -f *.so
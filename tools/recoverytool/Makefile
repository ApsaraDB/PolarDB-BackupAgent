include ../../common.mk

all : recoverytool
	cp recoverytool ../../run

recoverytool: recoverytool.go
	go build -o $@ $(FLAGS) $^

clean:
	-rm -f recoverytool
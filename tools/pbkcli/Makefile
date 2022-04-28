include ../../common.mk

all : pbkcli
	cp pbkcli ../../run

pbkcli: pbkcli.go util.go
	go build -o $@ $(FLAGS) $^

clean:
	-rm -f pbkcli
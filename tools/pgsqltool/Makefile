include ../../common.mk

all : pgsqltool
	cp pgsqltool ../../run

pgsqltool: pgsqltool.go
	go build -o $@ $(FLAGS) $^

clean:
	-rm -f pgsqltool
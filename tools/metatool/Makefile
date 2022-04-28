include ../../common.mk

all : metatool
	cp metatool ../../run

metatool: metatool.go
	go build -o $@ $(FLAGS) $^

clean:
	-rm -f metatool
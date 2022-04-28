include ../../common.mk

all : backupgc
	cp backupgc ../../run

backupgc: backupgc.go module.go
	go build -o $@ $(FLAGS) $^

clean:
	-rm -f backupgc
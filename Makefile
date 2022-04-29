.PHONY : clean
.PHONY : plugins

DATE := $(shell date '+%Y%m%d%H%M%S')
VERSION := $(shell cat VERSION |grep Version |awk '{print $$2}')
BRANCH := $(shell git branch | grep '\*' | cut -d ' ' -f 2 | cut -d '/' -f 2)

TARGET = backup_ctl

all: version libwaldump.a plugins $(TARGET) sometools
	mv backup_ctl run/backup_ctl

fmt:
	go fmt ./common/...
	go fmt ./loader/...
	go fmt ./plugins/...
	go fmt ./tools/...

clean:
	cd xlogparse; make clean
	cd test; make clean
	rm -f run/backup_ctl run/pgsqltool run/recoverytool run/*.so
	$(MAKE) -f Makefile.plugin clean
	$(MAKE) -f Makefile.tool clean

libwaldump.a:
	$(MAKE) -C xlogparse

$(TARGET): loader/main.go loader/ctrl.go loader/cmd.go loader/config.go loader/module.go loader/flock.go loader/version.go loader/fileserver.go
	go build -o $@ $^

sometools:
	$(MAKE) -f Makefile.tool

plugins:
	$(MAKE) -f Makefile.plugin

testwal:
	$(MAKE) -C test

rpc:
	cd loader/cmctrl; rm -rf ./polardb/*.pb.go
	cd loader/cmctrl; protoc --proto_path=./proto --go_out=. --go-grpc_out=. control_service.proto

rpm: clean
	make all
	rm -rf ~/rpmbuild/BUILD
	mkdir -p ~/rpmbuild/BUILD
	mkdir -p ~/rpmbuild/BUILD/script/dbstack
	mkdir -p ~/rpmbuild/SPECS
	cp run/* ~/rpmbuild/BUILD
	cp scripts/dbstack/* ~/rpmbuild/BUILD/script/dbstack
	cp VERSION ~/rpmbuild/BUILD
	cp CHANGELOG.md ~/rpmbuild/BUILD
	cp rpm/polardb-backup-agent.spec ~/rpmbuild/SPECS
	sed -i "s/RELEASEDATE/$(DATE)/g" ~/rpmbuild/SPECS/polardb-backup-agent.spec
	rpmbuild -bb ~/rpmbuild/SPECS/polardb-backup-agent.spec

version:
	./scripts/version_hack.sh

update-version:
	./scripts/update-version.sh
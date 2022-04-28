include ../../common.mk

TARGETS = pipeline.so pgpipeline.so increpipeline.so manager.so wal.so

all : $(TARGETS)
	cp *.so ../../run

.PHONY : clean
.PHONY : prepare

clean:
	-rm -f *.so

pipeline.so: pipeline.go pgdb.go task.go cmd.go tar.go  pg_control.go pipeline_common.go pipeline_ctrl.go pipeline_msg.go  fileclient.go
	go env -w CGO_CFLAGS="${CGO_CFLAGS}"
	go env -w CGO_LDFLAGS="${CGO_LDFLAGS}"
	go build -o $@ $(FLAGS) -buildmode=plugin $^

pgpipeline.so: pgpipeline.go pgdb.go task.go cmd.go tar.go  pg_control.go pipeline_common.go pipeline_ctrl.go pipeline_msg.go tar.go datapagemap.go encryption.go fileclient.go
	go env -w CGO_CFLAGS="${CGO_CFLAGS}"
	go env -w CGO_LDFLAGS="${CGO_LDFLAGS}"
	go build -o $@ $(FLAGS) -buildmode=plugin $^

increpipeline.so: increpipeline.go task.go cmd.go tar.go  pg_control.go pipeline_common.go pipeline_ctrl.go pipeline_msg.go tar.go pgdb.go encryption.go  fileclient.go
	go env -w CGO_CFLAGS="${CGO_CFLAGS}"
	go env -w CGO_LDFLAGS="${CGO_LDFLAGS}"
	go build -o $@ $(FLAGS) -buildmode=plugin $^

manager.so: manager.go cmd.go pipeline_msg.go pgdb.go shell.go task.go pipeline_common.go  fileclient.go
	go env -w CGO_CFLAGS="${CGO_CFLAGS}"
	go env -w CGO_LDFLAGS="${CGO_LDFLAGS}"
	go build -o $@ $(FLAGS) -buildmode=plugin $^

wal.so: wal.go cmd.go pipeline_msg.go pgdb.go shell.go task.go pipeline_common.go pipeline_ctrl.go  fileclient.go
	go env -w CGO_CFLAGS="${CGO_CFLAGS}"
	go env -w CGO_LDFLAGS="${CGO_LDFLAGS}"
	go build -o $@ $(FLAGS) -buildmode=plugin $^
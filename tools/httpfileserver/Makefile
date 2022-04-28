include ../../common.mk

all : http_file_server
	cp http_file_server ../../run

http_file_server: http_file_server.go
	go build -o $@ $(FLAGS) $^

clean:
	-rm -f http_file_server
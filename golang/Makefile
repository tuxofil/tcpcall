.PHONY: all test cover clean

ifndef ($(HOME))
    export HOME = /tmp
endif

DIR = src/tcpcall

all:

test:
	cd $(DIR) && go test -v -race -coverprofile=../.cover.out ./...

cover: test
	cd $(DIR) && go tool cover -html=../.cover.out

clean:
	rm -f src/.cover.out

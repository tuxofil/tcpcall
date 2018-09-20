.PHONY: all test fmt clean

all:

export GOPATH = $(shell pwd)

test:
	go test -v tcpcall

fmt:
	find . -type f -name \*.go -exec gofmt -w '{}' ';'

clean:

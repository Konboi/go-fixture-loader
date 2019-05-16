.PHONY: test lint deps clean

VET_FILE = $(shell find . -type f -name '*.go' | grep -v vendor)

test: deps lint

	go test -v ./...

lint: deps

	go vet -printfuncs=Criticalf,Infof,Warningf,Debugf,Tracef ${VET_FILE}
	golint -set_exit_status ./...

deps:

	go get golang.org/x/lint/golint

clean:

	go clean

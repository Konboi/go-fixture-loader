.PHONY: test lint deps

VET_FILE = $(shell find . -type f -name '*.go')

test: deps lint

	go test -v ./...

lint: deps

	go tool vet -all -printfuncs=Criticalf,Infof,Warningf,Debugf,Tracef ${VET_FILE}
	golint -set_exit_status ./...

deps:

	go get golang.org/x/lint/golint

clean:

	go clean

VET_FILE = $(shell find . -type f -name '*.go')

test: lint

	go test -v ./...

lint:

	go tool vet -all -printfuncs=Criticalf,Infof,Warningf,Debugf,Tracef ${VET_FILE}
	golint -set_exit_status ./...

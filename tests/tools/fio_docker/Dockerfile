FROM golang:1

RUN apt-get update \
    #
    # Install fio
    && apt-get install fio -y \
    #
    # Install gocode-gomod
    && go get -x -d github.com/stamblerre/gocode 2>&1 \
    && go build -o gocode-gomod github.com/stamblerre/gocode \
    && mv gocode-gomod $GOPATH/bin/
    
    
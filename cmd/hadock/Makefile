GOBIN=bin
GOCMD=go
GOBUILD=$(GOCMD) build
GOGET=$(GOCMD) get

SOURCE=src/github.com/midbel/mud/cmd/hadock/*go
BINARY=hadock
VERSION=

LDFLAGS = "-s -w -X github.com/midbel/cli.Version=$(VERSION) -X github.com/midbel/cli.BuildTime=`date -u +%FT%T`"

.PHONY: clean

build:
	$(GOBUILD) -ldflags $(LDFLAGS) -o $(GOBIN)/$(BINARY) $(SOURCE)

clean:
	rm -f $(GOBIN)/$(BINARY)

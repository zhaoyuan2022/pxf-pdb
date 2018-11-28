.PHONY: all depend build tar install test clean help
.DEFAULT_GOAL := build

TARGET := $(shell echo $${PWD\#\#*/})
PXF_SERVER_DIR := $(shell cd ../../../.. && pwd)
PXF_STAGING_DIR := ${PXF_SERVER_DIR}/build/stage/bin
PXF_SERVICE_SCRIPTS := ${PXF_SERVER_DIR}/pxf-service/src/scripts
PXF_VERSION := $(shell grep version= ${PXF_SERVER_DIR}/gradle.properties | cut -d= -f2)
LDFLAGS := -ldflags "-X ${TARGET}/cmd.version=${PXF_VERSION}"
GOPATH := ${PXF_SERVER_DIR}/pxf-cli/go

help:
	@echo
	@echo   "Possible targets"
	@echo	"  - all (depend, build, test, tar, install)"
	@echo	"  - depend - fetch all Go dependencies"
	@echo	"  - build - buid Go CLI and place in server/pxf-cli/go/bin/pxf-cli"
	@echo	"  - test - runs tests for PXF Go CLI"
	@echo	"  - install - install PXF CLI to $PXF_HOME/bin/pxf"
	@echo	"  - tar - bundle PXF CLI along with tomcat into a single tarball"
	@echo	"  - clean - remove pxf-cli binary from server/pxf-cli/go/bin"

all: test tar install

depend:
	@dep ensure -v

build: depend
	go build -v ${LDFLAGS}
	go install ${LDFLAGS}

tar: build
	mkdir -p ${PXF_STAGING_DIR}
	cp ${PXF_SERVICE_SCRIPTS}/pxf-service ${PXF_STAGING_DIR}/pxf
	cp ${GOPATH}/bin/pxf-cli ${PXF_STAGING_DIR}

install: build
	mkdir -p ${PXF_HOME}/bin
	cp ${PXF_SERVICE_SCRIPTS}/pxf-service ${PXF_HOME}/bin/pxf
	cp ${GOPATH}/bin/pxf-cli ${PXF_HOME}/bin

test: build
	ginkgo pxf cmd end_to_end

clean:
	go clean -i

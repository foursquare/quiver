VERSION=`git describe --always`
BUILD_TIME=`date`, by `whoami`, on `hostname`

LDFLAGS=-X \"main.version=${VERSION}\" -X \"main.buildTime=${BUILD_TIME}\"

build:
	go build -ldflags "${LDFLAGS}"

install:
	go install -ldflags "${LDFLAGS}"

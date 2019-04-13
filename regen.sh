#!/bin/bash
set -e
echo "Generating from gen/quiver.thrift..."
thrift -r --gen go gen/quiver.thrift
echo "Replacing apache.org imports with faster github mirrors..."
perl -pi -e 's#git.apache.org/thrift.git#github.com/apache/thrift#g' gen-go/gen/*.go
echo "Fixing imports..."
goimports -w gen-go/gen/*.go
echo "Overwriting with new version..."
cp gen-go/gen/*.go gen/
echo "Cleaning up..."
rm -rf gen-go

echo "Generating from gen-proto/quiver.proto..."
protoc --go_out=plugins=grpc:./ gen_proto/quiver.proto

echo "DIFFS:"
git diff gen*

#! /bin/sh

rm -rfv bin
mkdir -pv bin
VERSION="$(git describe --tags --always --dirty 2>/dev/null || echo dev)"
LDFLAGS="-X github.com/ydb-platform/loglugger/internal/buildinfo.Version=${VERSION}"

go build -ldflags "${LDFLAGS}" -o bin/loglugger-server ./cmd/server
go build -ldflags "${LDFLAGS}" -o bin/loglugger-client ./cmd/client
go build -ldflags "${LDFLAGS}" -o bin/loglugger-extractor ./cmd/extractor

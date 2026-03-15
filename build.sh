#! /bin/sh

mkdir -pv bin
VERSION="$(git describe --tags --always --dirty 2>/dev/null || echo dev)"
LDFLAGS="-X github.com/ydb-platform/loglugger/internal/buildinfo.Version=${VERSION}"

go build -ldflags "${LDFLAGS}" -o bin/server ./cmd/server
go build -ldflags "${LDFLAGS}" -o bin/client ./cmd/client

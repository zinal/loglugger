module github.com/ydb-platform/loglugger

go 1.24

require (
	github.com/coreos/go-systemd/v22 v22.5.0
	github.com/go-faster/city v1.0.1
	github.com/ydb-platform/ydb-go-sdk/v3 v3.127.3
	github.com/ydb-platform/ydb-go-yc v0.12.3
	gopkg.in/yaml.v3 v3.0.1
)

replace github.com/coreos/go-systemd/v22 => ./third_party/go-systemd

require (
	github.com/golang-jwt/jwt/v4 v4.5.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jonboulle/clockwork v0.5.0 // indirect
	github.com/klauspost/compress v1.18.4 // indirect
	github.com/yandex-cloud/go-genproto v0.61.0 // indirect
	github.com/ydb-platform/ydb-go-genproto v0.0.0-20260311095541-ebbf792c1180 // indirect
	github.com/ydb-platform/ydb-go-yc-metadata v0.6.1 // indirect
	golang.org/x/net v0.38.0 // indirect
	golang.org/x/sync v0.12.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.23.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20241015192408-796eee8c2d53 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241015192408-796eee8c2d53 // indirect
	google.golang.org/grpc v1.69.4 // indirect
	google.golang.org/protobuf v1.35.1 // indirect
)

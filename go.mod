module github.com/ydb-platform/loglugger

go 1.26.0

toolchain go1.26.5

require (
	github.com/coreos/go-systemd/v22 v22.5.0
	github.com/go-faster/city v1.0.1
	github.com/klauspost/compress v1.18.4
	github.com/ydb-platform/ydb-go-sdk/v3 v3.147.1
	github.com/ydb-platform/ydb-go-yc v0.12.3
	gopkg.in/yaml.v3 v3.0.1
)

replace github.com/coreos/go-systemd/v22 => ./third_party/go-systemd

require (
	github.com/golang-jwt/jwt/v4 v4.5.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jonboulle/clockwork v0.5.0 // indirect
	github.com/yandex-cloud/go-genproto v0.61.0 // indirect
	github.com/ydb-platform/ydb-go-genproto v0.0.0-20260428144813-1c07baab7f7b // indirect
	github.com/ydb-platform/ydb-go-yc-metadata v0.6.1 // indirect
	golang.org/x/net v0.48.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/text v0.32.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20251029180050-ab9386a59fda // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/grpc v1.78.0 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
)


build:
	go build

test:
	go test -cover .

cov:
	go test -coverprofile=/tmp/coverage.out
	go tool cover -html=/tmp/coverage.out

# Requires proto3
protoc:
	protoc net.proto --go_out=plugins=grpc:.

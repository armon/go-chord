
build:
	go build

test:
	go test .

cov:
	go test -coverprofile=/tmp/coverage.out .
	go tool cover -html=/tmp/coverage.out

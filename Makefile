.PHONY: test fmt vet check

test:
	go test ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

# What CI runs.
check: fmt vet test
	@test -z "$$(gofmt -l .)" || { echo "unformatted files:"; gofmt -l .; exit 1; }

BIN := foghorn
PKG := ./cmd/foghorn

.PHONY: build test vet lint tidy run clean

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BIN) $(PKG)

test:
	go test ./...

vet:
	go vet ./...

tidy:
	go mod tidy

run: build
	./$(BIN) -config examples/foghorn.yaml

clean:
	rm -f $(BIN)

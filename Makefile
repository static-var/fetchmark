.PHONY: build test tidy run lint fmt vet cover docker compose-up compose-down

BINARY := bin/fetchmark
PKG    := ./...

build:
	go build -o $(BINARY) ./cmd/fetchmark

test:
	go test -race -count=1 $(PKG)

cover:
	go test -race -coverprofile=coverage.out $(PKG)
	go tool cover -func=coverage.out | tail -1

tidy:
	go mod tidy

fmt:
	gofmt -s -w .

vet:
	go vet $(PKG)

run: build
	./$(BINARY)

docker:
	docker build -f deploy/Dockerfile -t fetchmark:dev .

compose-up:
	docker compose -f deploy/docker-compose.yml up --build

compose-down:
	docker compose -f deploy/docker-compose.yml down -v

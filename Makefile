.PHONY: build test tidy run lint fmt fmt-check tidy-check vet cover eval-check compat-sdk-check crawl-check pack-check pack-build-check pack-evidence-check pack-builder-image pack-evidence-image openpack-scale openpack-million-prototype peer-check docker compose-up compose-down

BINARY       := bin/fetchmark
CRAWL_BINARY := bin/fetchmark-crawl
PACK_BINARY  := bin/fetchmark-pack
PEER_BINARY  := bin/fetchmark-peer
PACK_EVIDENCE_BINARY := bin/fetchmark-pack-evidence
PKG          := ./...

build:
	go build -o $(BINARY) ./cmd/fetchmark
	go build -o $(CRAWL_BINARY) ./cmd/fetchmark-crawl
	go build -o $(PACK_BINARY) ./cmd/fetchmark-pack
	go build -o $(PEER_BINARY) ./cmd/fetchmark-peer
	go build -o $(PACK_EVIDENCE_BINARY) ./cmd/fetchmark-pack-evidence

test:
	go test -race -count=1 $(PKG)

cover:
	go test -race -coverprofile=coverage.out $(PKG)
	go tool cover -func=coverage.out | tail -1

tidy:
	go mod tidy

fmt:
	gofmt -s -w .

fmt-check:
	@test -z "$$(gofmt -l .)"

tidy-check:
	@go mod tidy
	@git diff --exit-code go.mod go.sum

vet:
	go vet $(PKG)

eval-check:
	go run ./cmd/fetchmark-eval -suite eval/queries.jsonl

compat-sdk-check:
	@test -n "$(COMPAT_SDK_PYTHON)" || (echo "set COMPAT_SDK_PYTHON to a Python environment with internal/api/testdata/compat-sdk-requirements.txt installed" >&2; exit 2)
	FETCHMARK_COMPAT_SDK_PYTHON="$(COMPAT_SDK_PYTHON)" go test -tags compat_sdk ./internal/api -run TestOfficialPythonSDKCompatibility -count=1 -v

crawl-check:
	go run ./cmd/fetchmark-crawl -config "$(CURDIR)/deploy/crawler.example.json" -dry-run

pack-check:
	go test ./cmd/fetchmark-pack ./internal/adapters/tufchannel ./internal/core/openpackchannel

pack-build-check:
	go test ./cmd/fetchmark-pack-build ./internal/adapters/ccindex ./internal/adapters/openpackbuilder ./internal/adapters/openpackindex ./internal/core/indexpack ./internal/core/indexpackadmission ./internal/core/indexpackselection

pack-evidence-check:
	go test ./cmd/fetchmark-pack-evidence ./internal/adapters/packevidence ./internal/core/indexpackadmission ./internal/adapters/egress ./internal/adapters/robots

pack-builder-image:
	docker build -f deploy/openpack-builder.Dockerfile -t fetchmark-pack-builder:dev .

pack-evidence-image:
	docker build -f deploy/openpack-evidence.Dockerfile -t fetchmark-pack-evidence:dev .

openpack-scale:
	./tools/openpack-scale.sh

openpack-million-prototype:
	./tools/openpack-million-prototype.sh

peer-check:
	go test ./cmd/fetchmark-peer

run: build
	./$(BINARY)

docker:
	docker build -f deploy/Dockerfile -t fetchmark:dev .

compose-up:
	docker compose -f deploy/docker-compose.yml up --build

compose-down:
	docker compose -f deploy/docker-compose.yml down -v

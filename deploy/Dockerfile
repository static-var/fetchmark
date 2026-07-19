############################
# Build
############################
FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/fetchmark ./cmd/fetchmark
RUN CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/fetchmark-crawl ./cmd/fetchmark-crawl
RUN CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/fetchmark-pack ./cmd/fetchmark-pack
RUN CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/fetchmark-peer ./cmd/fetchmark-peer
RUN mkdir -p /out/fetchmark-data /out/fetchmark-crawler-data

############################
# Runtime (distroless, static)
############################
FROM gcr.io/distroless/static:nonroot

COPY --from=build /out/fetchmark /fetchmark
COPY --from=build /out/fetchmark-crawl /fetchmark-crawl
COPY --from=build /out/fetchmark-pack /fetchmark-pack
COPY --from=build /out/fetchmark-peer /fetchmark-peer
COPY --from=build --chown=nonroot:nonroot /out/fetchmark-data /var/lib/fetchmark
COPY --from=build --chown=nonroot:nonroot /out/fetchmark-crawler-data /var/lib/fetchmark-crawler
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/fetchmark"]

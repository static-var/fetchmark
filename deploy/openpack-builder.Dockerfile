############################
# Offline publisher build
############################
FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
# Production publishers should pass an immutable source revision. The command
# replaces this local-only default with the executable SHA-256 in signed data.
RUN CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/fetchmark-pack-build ./cmd/fetchmark-pack-build

############################
# Offline publisher runtime
############################
FROM gcr.io/distroless/static:nonroot

COPY --from=build /out/fetchmark-pack-build /fetchmark-pack-build
USER nonroot:nonroot
ENTRYPOINT ["/fetchmark-pack-build"]

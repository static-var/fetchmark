############################
# Networked publisher evidence collector
############################
FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/fetchmark-pack-evidence ./cmd/fetchmark-pack-evidence

############################
# Separate networked runtime
############################
FROM gcr.io/distroless/static:nonroot

COPY --from=build /out/fetchmark-pack-evidence /fetchmark-pack-evidence
USER nonroot:nonroot
ENTRYPOINT ["/fetchmark-pack-evidence"]

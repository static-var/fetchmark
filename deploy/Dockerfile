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

############################
# Runtime (distroless, static)
############################
FROM gcr.io/distroless/static:nonroot

COPY --from=build /out/fetchmark /fetchmark
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/fetchmark"]

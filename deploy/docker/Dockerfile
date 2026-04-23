FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 GOFLAGS=-trimpath \
    go build -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/canary ./cmd/canary

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/canary /canary
USER nonroot:nonroot
EXPOSE 9090
ENTRYPOINT ["/canary"]
CMD ["-config", "/etc/canary/canary.yaml"]

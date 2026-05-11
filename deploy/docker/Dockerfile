FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOFLAGS=-trimpath \
    go build -ldflags="-s -w" \
    -o /out/foghorn ./cmd/foghorn

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/foghorn /foghorn
USER nonroot:nonroot
EXPOSE 8080 9090
ENTRYPOINT ["/foghorn"]
CMD ["-config", "/etc/foghorn/foghorn.yaml"]

FROM golang:1.26-alpine AS build
WORKDIR /src

# Dependencies first, so source edits do not re-download the module cache.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO off keeps the binary static, so it runs on a distroless base.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/pager .

# Distroless still ships CA certificates, which the HTTPS probes need.
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/pager /app/pager
COPY checks.yaml /app/checks.yaml

USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/app/pager"]

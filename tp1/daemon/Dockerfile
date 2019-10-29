FROM golang:alpine

WORKDIR /app
COPY storage/* /app/storage/
COPY daemon/* /app/daemon/

CMD go run daemon/responses.go daemon/daemon.go

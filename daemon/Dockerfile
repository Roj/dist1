FROM golang:alpine

WORKDIR /app
COPY storage/* /app/storage/
COPY daemon/daemon.go /app/daemon/

CMD go run daemon/daemon.go

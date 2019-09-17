FROM golang:alpine

WORKDIR /app
COPY structures.go daemon/daemon.go /app/

CMD go run structures.go daemon.go

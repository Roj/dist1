FROM golang:alpine

WORKDIR /app
COPY storage/* /app/storage/
COPY storage_server.go /app/

CMD go run storage_server.go

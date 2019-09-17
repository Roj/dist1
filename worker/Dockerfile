FROM golang:alpine

WORKDIR /app
COPY structures.go worker/worker.go /app/

CMD go run structures.go worker.go

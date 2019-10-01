FROM golang:alpine

WORKDIR /app
COPY worker/* /app/worker/
COPY storage/* /app/storage/
COPY worker_server.go /app/

CMD go run worker_server.go

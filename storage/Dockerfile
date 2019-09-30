FROM golang:alpine

WORKDIR /app
COPY structures.go db/db.go /app/

CMD go run structures.go db.go

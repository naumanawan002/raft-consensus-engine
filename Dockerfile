FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod ./
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/raft-demo ./cmd/cluster-demo

FROM alpine:3.20
RUN apk --no-cache add ca-certificates curl

WORKDIR /root/
COPY --from=builder /bin/raft-demo /usr/local/bin/raft-demo

EXPOSE 8095
ENTRYPOINT ["/usr/local/bin/raft-demo"]

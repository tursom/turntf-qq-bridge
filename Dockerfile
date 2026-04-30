FROM golang:1.24 AS builder

ENV GOTOOLCHAIN=auto

WORKDIR /src

COPY go.mod go.sum ./

RUN go mod edit -dropreplace github.com/tursom/turntf-go \
    && go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o /bin/turntf-qq-bridge ./cmd/turntf-qq-bridge

FROM alpine:latest

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /bin/turntf-qq-bridge /bin/turntf-qq-bridge

ENTRYPOINT ["/bin/turntf-qq-bridge"]

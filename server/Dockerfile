FROM golang:1.23-alpine AS builder
RUN apk add --no-cache gcc musl-dev git build-base pkgconfig libsodium-dev

ENV GOOS=linux

WORKDIR /etc/ente/

COPY ["go.mod", "go.sum", "./" ]
RUN go mod download

COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
  go build -o museum cmd/museum/main.go

FROM alpine:3.20
RUN apk add libsodium
COPY --from=builder /etc/ente/museum .
COPY configurations configurations
COPY migrations migrations
COPY mail-templates mail-templates

ARG GIT_COMMIT
ENV GIT_COMMIT=$GIT_COMMIT

CMD ["./museum"]

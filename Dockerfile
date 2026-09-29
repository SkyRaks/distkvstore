# FIRST STAGE
FROM golang:1.27-bookworm AS build-stage

WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /docker-gs-ping
# /////////////

# SECOND STAGE
FROM debian:trixie-slim AS build-release-stage

WORKDIR /

COPY --from=build-stage /docker-gs-ping /raft-node

EXPOSE 8080 

ENTRYPOINT ["/raft-node"]
# /////////////
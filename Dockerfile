# syntax=docker/dockerfile:1

##
## Build
##
FROM golang:1.16 AS builder
WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 go build -v -ldflags="-s -w -X main.buildVersion=$(git describe --tags --always --dirty)" -o /app/sql_efficiency_checker
RUN ls /app
##
## Deploy
##

FROM scratch
WORKDIR /
EXPOSE 8888
COPY --from=builder /app/docker.yaml /app/docker.yaml
COPY --from=builder /app/sql_efficiency_checker /app/sql_efficiency_checker
ENTRYPOINT  ["/app/sql_efficiency_checker" ]

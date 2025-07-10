FROM golang:1.24-alpine AS build
RUN apk --no-cache add ca-certificates && apk --no-cache upgrade

WORKDIR /build/

COPY go.mod go.sum /build/
RUN go mod download
COPY . /build
RUN go build .

FROM alpine:latest AS run
RUN apk add --update --no-cache ca-certificates && apk --no-cache upgrade

COPY --from=build /build/jaeger-kusto /go/bin/jaeger-kusto

ENTRYPOINT [ "/go/bin/jaeger-kusto" ]

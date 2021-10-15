FROM alpine:3.12

deps:
    ARG DISTRO
    IF [ "$DISTRO" = "alpine" ]
        FROM golang:1.16-alpine3.14
    ELSE IF [ "$DISTRO" = "ubuntu" ]
        FROM golang:1.16-bullseye
    ELSE
        RUN --no-cache echo "$DISTRO not supported" && false
    END
    WORKDIR /code
    COPY go.mod go.sum ./
    RUN go mod download
    # Output these back in case go mod download changes them.
    SAVE ARTIFACT go.mod AS LOCAL go.mod
    SAVE ARTIFACT go.sum AS LOCAL go.sum

build:
    ARG DISTRO
    FROM +deps
    COPY main.go .
    RUN go build -o build/boltdump main.go
    RUN test -n "$DISTRO"
    SAVE ARTIFACT build/boltdump /go-example AS LOCAL build/$DISTRO/boltdump

all:
    BUILD +build --DISTRO=alpine --DISTRO=ubuntu

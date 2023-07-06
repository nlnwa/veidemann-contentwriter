FROM golang:1.20 as build

WORKDIR /build

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go test ./...

# -trimpath remove file system paths from executable
# -ldflags arguments passed to go tool link:
#   -s disable symbol table
#   -w disable DWARF generation
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags "-s -w" .

FROM gcr.io/distroless/base
COPY --from=build /build/veidemann-contentwriter /

# api server
EXPOSE 8080/tcp
# prometheus metrics server
EXPOSE 9153/tcp

ENTRYPOINT ["/veidemann-contentwriter"]

FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -o /ingesta .

FROM alpine:3.20
COPY --from=build /ingesta /ingesta
USER nobody
EXPOSE 8080
ENTRYPOINT ["/ingesta"]

FROM golang:1.19-alpine3.16
WORKDIR /app
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
RUN go build -o service
CMD [ "./service" ]

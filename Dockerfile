FROM golang:1.20
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -v -o ./a.out
CMD [ "./a.out" ]

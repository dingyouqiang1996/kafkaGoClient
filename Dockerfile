FROM golang:1.25-alpine
RUN apk add --no-cache git
RUN apk add --no-cache librdkafka-dev
RUN git clone https://github.com/dingyouqiang1996/kafkaGoClient.git /app
WORKDIR /app
RUN go mod tidy && go build -o /app/kafkaGoClient .
CMD ["/app/kafkaGoClient"]
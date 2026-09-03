FROM golang:1.25 AS builder
RUN apt-get update && \
  apt-get install -y git librdkafka-dev && \
  rm -rf /var/lib/apt/lists/*    
RUN git clone https://github.com/dingyouqiang1996/kafkaGoClient.git /app
WORKDIR /app    
RUN go mod tidy && \
  CGO_ENABLED=1 go build -o /app/kafkaGoClient .
         
FROM debian:bookworm-slim  
RUN apt-get update && \
  apt-get install -y librdkafka1 && \
  rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/kafkaGoClient /app/kafkaGoClient        
WORKDIR /app    
CMD ["/app/kafkaGoClient"] 
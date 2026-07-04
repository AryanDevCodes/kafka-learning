## Kafka Order Notification (Learning Project)

Small Spring Boot project to learn the end-to-end flow of publishing and consuming Kafka events using Spring for Apache Kafka.

When you create an order via a REST API, the service:
1. "Saves" the order (simulated).
2. Publishes an `OrderCreatedEvent` to Kafka topic `order-created-topic`.
3. A Kafka consumer listens on the same topic and prints an "ORDER NOTIFICATION" to the console.

This repo is intentionally simple: it focuses on wiring, serialization, topics, consumer groups, and observing producer/consumer behavior.

## What You Learn Here

- Kafka topic creation via Spring (`NewTopic` bean).
- Producing messages with `KafkaTemplate` (key + JSON value).
- Consuming messages with a consumer group.
- JSON (de)serialization with Spring Kafka.
- Observing delivery metadata (partition + offset).
- Reproducing and understanding producer failures when Kafka is down/misconfigured.

## Components

- `OrderController`:
  - `POST /orders` creates an order and returns an `OrderResponse`.
- `OrderService`:
  - Creates `orderId` / `eventId`, builds `OrderCreatedEvent`, calls producer.
- `OrderEventProducer`:
  - Publishes event to Kafka asynchronously and logs success/failure.
- `OrderEventConsumer`:
  - Consumes `OrderCreatedEvent` and prints a notification to console.

## Configuration

Main settings are in `src/main/resources/application.yml`:

- `server.port: 8080`
- `spring.kafka.bootstrap-servers: localhost:9092`
- Producer:
  - `StringSerializer` for key
  - `JsonSerializer` for value
- Consumer:
  - `order-notification-group` group id
  - `earliest` offset reset
  - `JsonDeserializer` with trusted package `com.learn.aryandevcodes.kafkaorder.events`

## How To Run

1. Start Kafka (recommended via Docker Compose):

```bash
docker compose up -d
```

2. Run the Spring Boot app:

```bash
./mvnw spring-boot:run
```

3. Create an order:

```bash
curl -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"CUST-1","productName":"PENCIL","amount":12.50}'
```

You should see:
- Producer logs including partition/offset
- Consumer logs printing an "ORDER NOTIFICATION"

## How To See Producer Error Logs (Kafka Down)

This project publishes asynchronously. To reliably see producer failures in logs:

1. Stop Kafka (for example):

```bash
docker compose down
```

2. Keep the app running and call `POST /orders`.

If Kafka is unreachable, the producer callback will log an error (for example a timeout) instead of "Sent successfully".

Note: the HTTP API may still return an `OrderResponse` even when Kafka publish fails, because sending to Kafka is async and not awaited in the request thread. This is intentional for learning/demonstration.

## Notes / Next Experiments

- Add retry/backoff and dead-letter topics (DLT).
- Add idempotency / dedup logic on consumer side.
- Use a real DB transaction + outbox pattern (for a production-grade flow).

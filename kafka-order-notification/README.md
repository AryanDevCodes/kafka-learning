## Kafka Order Notification (Learning Project)

<p align="center">
  <b>Spring Boot • Apache Kafka • Event-Driven • Simple Producer-Consumer • Learning Fundamentals</b>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Java-Backend-orange" />
  <img src="https://img.shields.io/badge/Spring%20Boot-REST%20API-brightgreen" />
  <img src="https://img.shields.io/badge/Apache%20Kafka-Event%20Streaming-blue" />
  <img src="https://img.shields.io/badge/Spring%20Kafka-Integration-purple" />
</p>

---

## Overview

A **simple Spring Boot project** to learn the end-to-end flow of publishing and consuming Kafka events using Spring for Apache Kafka.

When you create an order via a REST API, the service:
1. Creates an order (simulated, stored in memory).
2. Publishes an `OrderCreatedEvent` to Kafka topic `order-created-topic`.
3. A Kafka consumer listens on the same topic and prints an "ORDER NOTIFICATION" to the console.

This project is **intentionally simple** and focuses on:
- Basic Kafka wiring and configuration
- Producer/Consumer serialization
- Topic management with Spring
- Observing delivery metadata (partition and offset)
- Understanding producer/consumer behavior
- Error handling when Kafka is unavailable

---

## Architecture

```text
┌──────────────┐
│   Client     │
└──────┬───────┘
       │ POST /orders
       ▼
┌──────────────────────────┐
│   OrderController        │
└──────┬───────────────────┘
       │
       ▼
┌──────────────────────────┐
│   OrderService           │
└──────┬───────────────────┘
       │
       ├──► Create OrderCreatedEvent
       │
       ▼
┌──────────────────────────┐
│ OrderEventProducer       │
└──────┬───────────────────┘
       │ KafkaTemplate.send()
       ▼
┌──────────────────────────┐
│  Kafka Topic             │
│  order-created-topic     │
└──────┬───────────────────┘
       │ Partition 0 / Offset N
       ▼
┌──────────────────────────┐
│ OrderEventConsumer       │
└──────┬───────────────────┘
       │
       ▼
┌──────────────────────────┐
│  Console Output          │
│  ORDER NOTIFICATION      │
└──────────────────────────┘
```

---

## What You Learn Here

✅ Kafka topic creation via Spring (`NewTopic` bean)  
✅ Producing messages with `KafkaTemplate` (key + JSON value)  
✅ Consuming messages with a consumer group  
✅ JSON (de)serialization with Spring Kafka  
✅ Observing delivery metadata (partition + offset)  
✅ Producer callbacks for success/failure handling  
✅ Understanding consumer group behavior  
✅ Reproducing and debugging producer failures  

---

## Components

| Component | Responsibility |
|-----------|-----------------|
| **OrderController** | REST API endpoint `POST /orders` |
| **OrderService** | Business logic: create order, build event, call producer |
| **OrderEventProducer** | Publishes `OrderCreatedEvent` to Kafka asynchronously |
| **OrderEventConsumer** | Consumes `OrderCreatedEvent` from Kafka topic |
| **OrderCreatedEvent** | DTO representing the order creation event |

### Code Structure

```java
// OrderController
@PostMapping("/orders")
public OrderResponse createOrder(@RequestBody OrderRequest request) {
    return orderService.createOrder(request);
}

// OrderService
public OrderResponse createOrder(OrderRequest request) {
    String orderId = UUID.randomUUID().toString();
    OrderCreatedEvent event = new OrderCreatedEvent(...);
    orderEventProducer.publish(event);
    return new OrderResponse(orderId, ...);
}

// OrderEventProducer
public void publish(OrderCreatedEvent event) {
    kafkaTemplate.send(TOPIC_NAME, orderId, event);
}

// OrderEventConsumer
@KafkaListener(topics = "order-created-topic", groupId = "order-notification-group")
public void consume(OrderCreatedEvent event) {
    System.out.println("ORDER NOTIFICATION: " + event);
}
```

---

## Configuration

Main settings are in `src/main/resources/application.yml`:

```yaml
server:
  port: 8080

spring:
  kafka:
    bootstrap-servers: localhost:9092
    
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
    
    consumer:
      group-id: order-notification-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: com.learn.aryandevcodes.kafkaorder.events
```

---

## Event Structure

```json
{
  "eventId": "550e8400-e29b-41d4-a716-446655440000",
  "orderId": "ORD-12345",
  "customerId": "CUST-1",
  "productName": "PENCIL",
  "amount": 12.50,
  "timestamp": "2026-07-04T15:45:00Z"
}
```

---

## How To Run

### 1. Start Kafka & Zookeeper

```bash
cd kafka-order-notification
docker compose up -d
```

### 2. Run the Spring Boot app

```bash
./mvnw spring-boot:run
```

You should see logs:
```
Listening on port 8080
Kafka Bootstrap Servers: localhost:9092
Consumer group: order-notification-group
```

### 3. Create an order

```bash
curl -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"CUST-1","productName":"PENCIL","amount":12.50}'
```

### 4. Observe output

**Producer Logs:**
```
[1] CONTROLLER → OrderRequest received
[2] SERVICE → Processing order creation
[3] PRODUCER → Publishing OrderCreatedEvent
     Event ID: 550e8400-e29b-41d4-a716-446655440000
     Sent successfully to partition 0, offset 5
```

**Consumer Logs:**
```
[CONSUMER] ORDER NOTIFICATION received:
     Order ID: ORD-12345
     Customer: CUST-1
     Product: PENCIL
     Amount: 12.50
```

---

## How To See Producer Error Logs (Kafka Down)

This project publishes **asynchronously**. To reliably see producer failures in logs:

### 1. Stop Kafka

```bash
docker compose down
```

### 2. Keep the app running and call `POST /orders`

```bash
curl -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"CUST-1","productName":"PENCIL","amount":12.50}'
```

### 3. Check logs for producer error

```
[PRODUCER ERROR] Failed to publish event:
     Reason: Broker connection error
     Message: The broker is unavailable
```

### Important Note

The HTTP API may still return an `OrderResponse` even when Kafka publish fails, because sending to Kafka is **async and not awaited** in the request thread. This is **intentional for learning/demonstration** purposes.

---

## Key Learning Points

### Producer-Consumer Pattern
- Producer (OrderEventProducer) sends events to a topic
- Consumer (OrderEventConsumer) reads from the same topic
- They are **loosely coupled** — don't know about each other

### Async Publishing
- `KafkaTemplate.send()` is **non-blocking**
- Callback handles success/failure
- Application doesn't wait for Kafka acknowledgment

### Serialization
- **Producer**: Java object → JSON (JsonSerializer)
- **Consumer**: JSON → Java object (JsonDeserializer)
- Configured in `application.yml`

### Consumer Groups
- `order-notification-group` is the consumer group ID
- Multiple instances can join the same group for load balancing
- Kafka distributes partitions across instances

### Offset Management
- `auto-offset-reset: earliest` means: if no previous offset exists, start from the beginning
- Useful for learning and replaying events
- For production: consider `latest` to avoid reprocessing old events

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Connection refused to localhost:9092 | Run `docker compose up -d` and verify Kafka is running |
| Event not consumed | Check `trusted.packages` configuration includes your event package |
| Consumer group not created | Consumer group is auto-created on first consumer connection |
| Duplicate events on restart | Due to `auto-offset-reset: earliest`, set to `latest` for production |

---

## Comparison: kafka-order-notification vs DB-level-kafka-order-notification

| Feature | This Project | DB-Level Project |
|---------|-------------|------------------|
| Database | None (in-memory) | SQLite + JPA |
| Persistence | No | Yes |
| Idempotency | None | Database-level |
| Deduplication | None | Event store tracking |
| Transaction Safety | None | Atomic DB operations |
| Production Ready | Learning only | Near-production |

---

## Next Steps & Experiments

- [ ] Add retry/backoff logic with `@Retry` annotation
- [ ] Implement dead-letter topics (DLT) for failed events
- [ ] Add timestamps and tracing for event flow
- [ ] Implement idempotency using database (see DB-level project)
- [ ] Add error handling and graceful degradation
- [ ] Monitor Kafka metrics with Micrometer
- [ ] Add unit and integration tests

---

## Tech Stack

| Technology | Purpose |
|-----------|---------|
| Java | Backend language |
| Spring Boot | REST API & service layer |
| Spring Kafka | Kafka integration |
| Apache Kafka | Event streaming platform |
| Docker Compose | Local Kafka setup |
| Maven | Build tool |

---

## Author

**Aryan Raj**  
Backend Developer | Java | Spring Boot | Kafka

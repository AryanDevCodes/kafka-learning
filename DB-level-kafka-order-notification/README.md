# DB-Level Kafka Order Notification

<p align="center">
  <b>Spring Boot • Apache Kafka • SQLite • Idempotency • Event Deduplication • Transactional Guarantees</b>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Java-Backend-orange" />
  <img src="https://img.shields.io/badge/Spring%20Boot-REST%20API-brightgreen" />
  <img src="https://img.shields.io/badge/Apache%20Kafka-Event%20Streaming-blue" />
  <img src="https://img.shields.io/badge/SQLite-Persistence-lightblue" />
  <img src="https://img.shields.io/badge/Idempotency-Database%20Level-purple" />
  <img src="https://img.shields.io/badge/JPA-ORM-brightgreen" />
</p>

---

## Overview

This is an **advanced version** of the kafka-order-notification project that demonstrates **database-level event processing** and **idempotency**.

When you create an order via a REST API:
1. The order is **persisted to SQLite database** using JPA/Hibernate.
2. An `OrderCreatedEvent` is **published to Kafka topic**.
3. A Kafka **consumer** listens on the topic and processes the event.
4. **Idempotency checks** ensure events are not processed twice in the database.
5. A **processed event store** tracks which events have already been handled.

This project focuses on:
- Database persistence with JPA and Hibernate
- Event deduplication and idempotency patterns
- Kafka offset management with consumer groups
- Transactional consistency between database and Kafka
- Production-ready error handling

---

## Key Concepts

### 1. Database Persistence
Orders are saved to SQLite using Spring Data JPA, ensuring data survives application restarts.

### 2. Idempotency at Database Level
Processed events are stored in a database table. Before processing an event from Kafka, the consumer checks if it has already been processed.

### 3. Event Deduplication
The `ProcessedEventStore` or `IdempotencyRepository` tracks `eventId` to prevent duplicate processing.

### 4. Transactional Consistency
Database transactions ensure orders and processed events are atomically updated together.

---

## Architecture Flow

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
       ├──► Save to Database (SQLite)
       │
       ├──► Create OrderCreatedEvent
       │
       ▼
┌──────────────────────────┐
│ OrderEventProducer       │
└──────┬───────────────────┘
       │ Publishes to Kafka
       ▼
┌──────────────────────────┐
│  Kafka Topic             │
│  order-created-topic     │
└──────┬───────────────────┘
       │
       ▼
┌──────────────────────────┐
│ OrderEventConsumer       │
└──────┬───────────────────┘
       │
       ├──► Check IdempotencyRepository
       │
       ├──► If not processed:
       │    - Call NotificationService
       │    - Mark event as processed
       │
       ▼
┌──────────────────────────┐
│  SQLite Database         │
│  (Processed Events)      │
└──────────────────────────┘
```

---

## Components

| Component | Responsibility |
|-----------|-----------------|
| **OrderController** | REST API endpoint to create orders |
| **OrderService** | Business logic: save order, create event, publish to Kafka |
| **OrderEventProducer** | Publishes `OrderCreatedEvent` to Kafka asynchronously |
| **OrderEventConsumer** | Consumes events from Kafka, checks idempotency, processes |
| **NotificationService** | Handles order notifications (logging, email, SMS, etc.) |
| **IdempotencyRepository** | JPA repository to track processed events |
| **ProcessedEventStore** | Database table storing processed event IDs |
| **OrderEntity** | JPA entity representing persisted order data |

---

## Database Schema

### Orders Table
```sql
CREATE TABLE orders (
  id BIGINT PRIMARY KEY,
  customer_id VARCHAR(255),
  product_name VARCHAR(255),
  amount DECIMAL(10, 2),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Processed Events Table
```sql
CREATE TABLE processed_events (
  id BIGINT PRIMARY KEY,
  event_id VARCHAR(255) UNIQUE,
  processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
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
  
  datasource:
    url: jdbc:sqlite:./order-notification.db
    driver-class-name: org.sqlite.JDBC
  
  jpa:
    database-platform: org.hibernate.community.dialect.SQLiteDialect
    hibernate:
      ddl-auto: update
    show-sql: true
```

---

## How To Run

### 1. Start Kafka

```bash
docker compose up -d
```

### 2. Run the Spring Boot app

```bash
./mvnw spring-boot:run
```

### 3. Create an order

```bash
curl -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"CUST-1","productName":"PENCIL","amount":12.50}'
```

### 4. Verify database persistence

```bash
sqlite3 order-notification.db "SELECT * FROM orders;"
sqlite3 order-notification.db "SELECT * FROM processed_events;"
```

---

## Idempotency Pattern

### First Event Processing
1. Consumer receives `eventId = 'evt-001'` from Kafka
2. Queries `IdempotencyRepository` for `eventId`
3. **Not found** → Proceed with processing
4. Insert order into database
5. Save `eventId` to processed_events table
6. Return success

### Duplicate Event
1. Consumer receives same `eventId = 'evt-001'` again (e.g., after restart with earliest offset)
2. Queries `IdempotencyRepository` for `eventId`
3. **Found** → Skip processing, log "Already processed"
4. No duplicate order created
5. Database remains consistent

---

## API Examples

### Create Order (First Request)
```bash
curl -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{
    "customerId": "CUST-101",
    "productName": "Laptop",
    "amount": 55000.00,
    "clientRequestId": "REQ-1001"
  }'
```

### Response
```json
{
  "orderId": "ORD-9ad8eb8c-a795-490d-8004-26c862917194",
  "status": "ORDER_CREATED",
  "message": "Order created successfully"
}
```

### Duplicate Request (Same clientRequestId)
```bash
curl -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{
    "customerId": "CUST-101",
    "productName": "Laptop",
    "amount": 55000.00,
    "clientRequestId": "REQ-1001"
  }'
```

### Response (Same as before - Duplicate Detected)
```json
{
  "orderId": "ORD-9ad8eb8c-a795-490d-8004-26c862917194",
  "status": "DUPLICATE_REQUEST",
  "message": "Order already exists for this request ID"
}
```

### Console Output for First Request
```
[0] POST /orders called
[1] Order creation started
[IDEMPOTENCY] ClientRequestId = REQ-1001
[2] New order created in database
Order ID: ORD-9ad8eb8c-a795-490d-8004-26c862917194
[3] OrderCreatedEvent created
[Producer] Sending order event to Kafka: ...
[Producer] Sent successfully to Partition: 1, Offset: 0
[IDEMPOTENCY] Request marked as COMPLETED

[CONSUMER] Event received from Kafka
[CONSUMER] Event ID: EVT-30981a43-d40c-4597-9382-116cbc283c29

================================
ORDER NOTIFICATION
================================
Order created successfully.
Event ID: EVT-30981a43-d40c-4597-9382-116cbc283c29
Order ID: ORD-9ad8eb8c-a795-490d-8004-26c862917194
Customer ID: CUST-101
Product: Laptop
Amount: 55000.00
Status: ORDER_CREATED
================================

[CONSUMER] Notification sent successfully
```

### Console Output for Duplicate Request
```
[0] POST /orders called
[1] Order creation started
[IDEMPOTENCY] ClientRequestId = REQ-1001
[IDEMPOTENCY] Duplicate request detected
```

# Kafka Learning Projects

A collection of Kafka-based projects for learning and studying Apache Kafka patterns, architecture, and best practices.

## Projects

### 1. kafka-banking-events
A Spring Boot application demonstrating Kafka usage for banking event processing:
- Payment event production and consumption
- Event-driven architecture
- Kafka topic management
- Docker Compose setup for local development

#### Quick Start
```bash
cd kafka-banking-events
docker-compose up
mvn spring-boot:run
```

### 2. kafka-order-notification
A Spring Boot application for learning end-to-end Kafka event publishing and consuming:
- REST API to create orders and trigger Kafka events
- Order event production and consumption patterns
- JSON serialization/deserialization with Spring Kafka
- Topic creation via Spring configuration
- Observing delivery metadata (partition and offset)
- Error handling and logging

#### Quick Start
```bash
cd kafka-order-notification
docker-compose up
./mvnw spring-boot:run
```

Create an order:
```bash
curl -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"CUST-1","productName":"PENCIL","amount":12.50}'
```

### 3. DB-level-kafka-order-notification
An advanced Spring Boot application extending kafka-order-notification with database-level event processing:
- Database-level order persistence with JPA/Hibernate
- Idempotency checks to prevent duplicate order processing
- Event deduplication using processed event store
- Order notifications service for order status tracking
- Kafka consumer with automatic offset reset
- Error handling and transactional guarantees
- Docker Compose setup with database support

#### Quick Start
```bash
cd DB-level-kafka-order-notification
docker-compose up
./mvnw spring-boot:run
```

Create an order:
```bash
curl -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"CUST-1","productName":"PENCIL","amount":12.50}'
```

## Getting Started

Each project folder contains its own setup instructions and documentation.

## Technologies Used
- Apache Kafka
- Spring Boot
- Docker & Docker Compose
- Maven


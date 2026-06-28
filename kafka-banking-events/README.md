# Kafka Based Payment Notification System

<p align="center">
  <b>Spring Boot • Apache Kafka • Event Driven Architecture • Basic Idempotency • Payment Notification Flow</b>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Java-Backend-orange" />
  <img src="https://img.shields.io/badge/Spring%20Boot-REST%20API-brightgreen" />
  <img src="https://img.shields.io/badge/Apache%20Kafka-Event%20Streaming-blue" />
  <img src="https://img.shields.io/badge/Idempotency-Basic%20Duplicate%20Protection-purple" />
  <img src="https://img.shields.io/badge/Architecture-Event%20Driven-black" />
</p>

---

## Overview

This project demonstrates a **Kafka-based payment notification system** using **Spring Boot**.

When a payment is completed, the payment service creates a `PaymentCompletedEvent` and publishes it to a Kafka topic. The notification consumer reads this event and sends a payment success notification to the user.

The project also includes **basic idempotency** using `clientRequestId`, so repeated requests with the same client request ID return the previously created event instead of creating a new payment event again.

---

## Main Idea

```text
Client → Payment Service → Kafka Topic → Notification Consumer → User Notification
```

The payment service is responsible for payment processing. Kafka is responsible for carrying the payment event. The notification consumer is responsible for reading the event and sending the notification.

---

## Architecture Diagram

<img width="1536" height="1024" alt="ChatGPT Image Jun 28, 2026, 05_38_12 PM" src="https://github.com/user-attachments/assets/1dbaa029-142b-45bf-8929-61b2ded8f012" />

---

## Basic Idempotency Diagram

<img width="1800" height="1100" alt="image" src="https://github.com/user-attachments/assets/dc6a32ca-54a6-435c-9111-f9e76bc7fd2d" />

---
## Components Used

| Component | Meaning | Responsibility |
|---|---|---|
| Client | Frontend, Postman, mobile app, or another service | Sends payment request |
| Payment Controller | REST API layer | Receives request and calls payment service |
| Payment Service | Business layer | Handles payment completion and idempotency check |
| `clientRequestId` | Unique request identifier | Used to detect duplicate payment requests |
| `ConcurrentHashMap` | In-memory request store | Stores already processed requests |
| `PaymentCompletedEvent` | Event object | Carries payment completion details |
| Kafka Producer | Event publisher | Publishes payment event to Kafka topic |
| Kafka Topic | Message channel | Stores payment completed events |
| Kafka Partition | Internal topic division | Maintains message order for same key |
| Kafka Offset | Message position | Identifies exact message location |
| Notification Consumer | Event reader | Reads event from Kafka |
| Notification Service | Communication layer | Sends SMS, email, or push notification |

---

## PaymentCompletedEvent Structure

```json
{
  "eventId": "unique-event-id",
  "transactionId": "TXN-unique-transaction-id",
  "userId": 143343354345400,
  "senderAccountId": 10,
  "receiverAccountId": 20541,
  "amount": 8008680,
  "occurredAt": "2026-06-28T13:46:37.802"
}
```

---

## Basic Idempotency Implementation

The service uses a `ConcurrentHashMap` to store already processed requests.

```java
private final Map<String, PaymentCompletedEvent> processedRequests = new ConcurrentHashMap<>();
```

The key is:

```java
request.clientRequestId()
```

The value is:

```java
PaymentCompletedEvent
```

This means:

```text
clientRequestId → PaymentCompletedEvent
```

If the same `clientRequestId` comes again, the service returns the previously created event.

---

## PaymentService With Basic Idempotency

```java
package com.learn.aryandevcodes.service;

@Service
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentEventProducer paymentEventProducer;
    private final Map<String, PaymentCompletedEvent> processedRequests = new ConcurrentHashMap<>();

    public PaymentCompletedEvent completePayment(PaymentRequest request) {

        System.out.println("[3] SERVICE → PaymentService started payment completion");

        if (processedRequests.containsKey(request.clientRequestId())) {
            System.out.println("    [IDEMPOTENCY] Duplicate request detected: " + request.clientRequestId());
            return processedRequests.get(request.clientRequestId());
        }

        return processedRequests.computeIfAbsent(request.clientRequestId(), id -> {
            System.out.println("    [IDEMPOTENCY] Processing NEW request: " + id);
            System.out.println("    Simulating banking operations:");
            System.out.println("    - Validating sender account");
            System.out.println("    - Validating receiver account");
            System.out.println("    - Checking balance");
            System.out.println("    - Creating transaction record");
            System.out.println("    - Creating ledger entries");

            PaymentCompletedEvent event = new PaymentCompletedEvent(
                    UUID.randomUUID().toString(),
                    "TXN-" + UUID.randomUUID(),
                    request.userId(),
                    request.senderAccountId(),
                    request.receiverAccountId(),
                    request.amount(),
                    LocalDateTime.now()
            );

            System.out.println("[4] SERVICE → PaymentCompletedEvent created");
            System.out.println("    Event ID       = " + event.eventId());
            System.out.println("    Transaction ID = " + event.transactionId());
            System.out.println("    User ID        = " + event.userId());
            System.out.println("    Amount         = ₹" + event.amount());

            paymentEventProducer.publish(event);

            System.out.println("[8] SERVICE → Producer call completed, returning event to controller");

            return event;
        });
    }
}
```

---

## How Idempotency Works

### First Request

```text
clientRequestId = REQ-1001
```

The service checks the map.

```text
REQ-1001 does not exist
```

So the service:

1. Processes the payment
2. Creates a new `PaymentCompletedEvent`
3. Publishes the event to Kafka
4. Stores the event in `processedRequests`
5. Returns the event to the controller

---

### Duplicate Request

```text
clientRequestId = REQ-1001
```

The service checks the map.

```text
REQ-1001 already exists
```

So the service:

1. Does not process payment again
2. Does not create a new transaction
3. Does not create new ledger entries
4. Does not publish a duplicate Kafka event
5. Returns the old `PaymentCompletedEvent`

---

## Why `computeIfAbsent()` Is Used

`computeIfAbsent()` is used because it safely checks whether the key is already present.

If the key is absent, it executes the given logic and stores the result.

```java
processedRequests.computeIfAbsent(request.clientRequestId(), id -> {
    // process payment only once
});
```

This helps avoid duplicate processing for the same request ID.

---

## Why `ConcurrentHashMap` Is Used

`ConcurrentHashMap` is used because multiple requests can come at the same time.

It is better than a normal `HashMap` in a multi-threaded web application.

```text
HashMap              → Not safe for concurrent access
ConcurrentHashMap    → Safer for concurrent access
```

---

## Important Note

This is **basic idempotency** because the processed request data is stored only in memory.

```text
private final Map<String, PaymentCompletedEvent> processedRequests
```

This means the data will be lost when the application restarts.

For production systems, idempotency keys should be stored in a database or distributed cache.

Recommended production options:

| Storage | Use Case |
|---|---|
| Database table | Permanent idempotency records |
| Redis | Fast distributed idempotency storage |
| Unique database constraint | Prevent duplicate transaction creation |
| TTL-based cache | Expire old idempotency keys after fixed time |

---

## Production-Level Idempotency Idea

In a real banking system, idempotency should be handled like this:

```text
1. Client sends Idempotency-Key
2. Backend checks key in database
3. If key exists, return previous response
4. If key does not exist, create PROCESSING record
5. Process payment safely
6. Save final response against same key
7. Return response
```

---

## Why Kafka Is Used

Without Kafka:

```text
Payment Service → Notification Service
```

This creates tight coupling.

If the notification service is slow or unavailable, the payment service may also be affected.

With Kafka:

```text
Payment Service → Kafka → Notification Service
```

Now the payment service only publishes an event. The notification service consumes the event separately.

---

## Advantages

| Advantage | Explanation |
|---|---|
| Loose Coupling | Payment service and notification service are independent |
| Asynchronous Processing | Payment service does not wait for notification delivery |
| Scalability | Kafka can handle many payment events |
| Reliability | Kafka stores events until consumers process them |
| Ordered Processing | Same user events can go to the same partition |
| Basic Duplicate Protection | `clientRequestId` prevents repeated processing in memory |

---

## Suggested Project Structure

```text
src
└── main
    └── java
        └── com.learn.aryandevcodes
            ├── controller
            │   └── PaymentController.java
            ├── service
            │   └── PaymentService.java
            ├── event
            │   └── PaymentCompletedEvent.java
            ├── producer
            │   └── PaymentEventProducer.java
            ├── consumer
            │   └── NotificationConsumer.java
            ├── dto
            │   └── PaymentRequest.java
            └── config
                └── KafkaConfig.java
```

---

## Sample API Request

```http
POST /api/payments/complete
Content-Type: application/json
```

```json
{
  "clientRequestId": "REQ-1001",
  "userId": 143343354345400,
  "senderAccountId": 10,
  "receiverAccountId": 20541,
  "amount": 8008680
}
```

---

## Sample API Response

```json
{
  "eventId": "e598c9ae-2953-428c-814c-01ae9c6e0b56",
  "transactionId": "TXN-004f36b4-3a3c-4d82-8ab8-78caec719568",
  "userId": 143343354345400,
  "senderAccountId": 10,
  "receiverAccountId": 20541,
  "amount": 8008680,
  "occurredAt": "2026-06-28T13:46:37.802"
}
```

---

## Learning Summary

This project explains how Kafka can be used in a payment notification system. The client sends a payment request with a `clientRequestId`. The payment service checks whether this request ID has already been processed. If it is a new request, the service validates the payment, creates transaction and ledger records, creates a `PaymentCompletedEvent`, publishes it to Kafka, stores the result in memory, and returns the event. If the same request comes again, the service returns the previously created event and avoids duplicate processing.

Kafka helps separate payment processing from notification handling. The producer publishes the event to the Kafka topic, and the notification consumer reads the event asynchronously to send SMS, email, or push notification.

---

## One-Line Definition

Kafka is used in this payment system to publish payment-completed events asynchronously, while basic idempotency using `clientRequestId` prevents duplicate payment processing for repeated requests.

---

## Tech Stack

| Technology | Used For |
|---|---|
| Java | Backend programming |
| Spring Boot | REST API and service layer |
| Apache Kafka | Event streaming |
| Kafka Producer | Publishing payment events |
| Kafka Consumer | Reading payment events |
| ConcurrentHashMap | Basic in-memory idempotency |
| Database | Transaction and ledger persistence |

---

## Author

**Aryan Raj**  
Backend Developer | Java | Spring Boot | Kafka | PostgreSQL

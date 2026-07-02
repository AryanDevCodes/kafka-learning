package com.learn.aryandevcodes.kafkaorder.events;

public record OrderCreatedEvent(
    String eventId,
    String orderId, 
    String customerId,
    String productName,
    double amount,
    String orderStatus
) {}

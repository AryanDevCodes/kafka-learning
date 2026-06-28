package com.learn.aryandevcodes.events;

import java.math.BigDecimal;
import java.time.LocalDateTime;
/*
    This is the event that will be published when a payment is completed
    Carried out by the Kafka
 */
public record PaymentCompletedEvent(
        String eventId,
        String transactionId,
        Long userId,
        Long senderAccountId,
        Long receiverAccountId,
        BigDecimal amount,
        LocalDateTime occurredAt
) {}

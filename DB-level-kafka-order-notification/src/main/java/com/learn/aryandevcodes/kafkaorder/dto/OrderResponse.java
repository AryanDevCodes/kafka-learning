package com.learn.aryandevcodes.kafkaorder.dto;

public record OrderResponse(
    String orderId,
    String status,
    String message
) {
}

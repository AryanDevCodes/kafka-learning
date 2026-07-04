package com.learn.aryandevcodes.kafkaorder.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

public record OrderRequest(
    @NotBlank(message = "Customer ID cannot be blank")
    String customerId,

    @NotBlank(message = "Product name cannot be blank")
    String productName,

    @NotNull(message = "Amount cannot be null")
    @Positive(message = "Amount must be a positive value")
    Double amount,

    // Adding clientRequestId to the OrderRequest DTO for idempotency
    @NotBlank(message = "Request ID cannot be blank")
    String clientRequestId
) {
} 
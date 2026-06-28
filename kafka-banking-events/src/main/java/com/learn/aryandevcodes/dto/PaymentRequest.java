package com.learn.aryandevcodes.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;

public record PaymentRequest(
        @NotNull(message = "User id is required")
        Long userId,
        @NotNull(message = "Sender account id is required")
        Long senderAccountId,
        @NotNull(message = "Receiver account id is required")
        Long receiverAccountId,

        @NotNull(message = "Amount is required")
        @DecimalMin(value = "0.01", message = "Amount must be at least 1")
        BigDecimal amount,
        @NotNull(message = "Client request id is required")
        String clientRequestId
) {
}

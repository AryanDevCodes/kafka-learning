package com.learn.aryandevcodes.service;

import com.learn.aryandevcodes.dto.PaymentRequest;
import com.learn.aryandevcodes.producer.PaymentEventProducer;
import com.learn.aryandevcodes.events.PaymentCompletedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

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
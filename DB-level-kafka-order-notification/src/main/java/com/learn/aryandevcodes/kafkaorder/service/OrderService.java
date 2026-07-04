package com.learn.aryandevcodes.kafkaorder.service;

import com.learn.aryandevcodes.kafkaorder.repository.IdempotencyRepository;
import com.learn.aryandevcodes.kafkaorder.dto.OrderRequest;
import com.learn.aryandevcodes.kafkaorder.dto.OrderResponse;
import com.learn.aryandevcodes.kafkaorder.events.OrderCreatedEvent;
import com.learn.aryandevcodes.kafkaorder.model.IdempotencyRecord;
import com.learn.aryandevcodes.kafkaorder.model.IdempotencyStatus;
import com.learn.aryandevcodes.kafkaorder.producer.OrderEventProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderEventProducer orderEventProducer;
    private final IdempotencyRepository idempotencyRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public OrderResponse createOrder(OrderRequest request) {

        System.out.println("[1] Order creation started");
        System.out.println(" [ IDEMPOTENCY ] ClientRequestId  = " + request.clientRequestId());

        var existingOptionalRecord = idempotencyRepository.findByClientRequestId(request.clientRequestId());

        if (existingOptionalRecord.isPresent()){
            IdempotencyRecord existingRecord = existingOptionalRecord.get();
            if (existingRecord.getStatus() == IdempotencyStatus.COMPLETED){
                System.out.println("[IDEMPOTENCY] Duplicate request detected");
                return readResponse(existingRecord.getResponseBody());
            }

            if (existingRecord.getStatus() == IdempotencyStatus.PROCESSING){
                System.out.println("[IDEMPOTENCY] Request is already processing");

                return new OrderResponse(
                        existingRecord.getOrderId(),
                        "PROCESSING",
                        "This request is already being processed"
                );
            }

            if (existingRecord.getStatus() == IdempotencyStatus.FAILED){
                System.out.println("[IDEMPOTENCY] Request failed previously. Re-processing the request.");
                return new OrderResponse(
                        existingRecord.getOrderId(),
                        "FAILED",
                        "This request failed previously. Please try again."
                );
            }
        }

        IdempotencyRecord record = IdempotencyRecord.builder()
                .clientRequestId(request.clientRequestId())
                .status(IdempotencyStatus.PROCESSING)
                .build();

        idempotencyRepository.save(record);

        try {
            String orderId = "ORD-" + UUID.randomUUID();
            String eventId = "EVT-" + UUID.randomUUID();

            System.out.println("[2] New order created in database ");
            System.out.println("Order ID: " + orderId);

            OrderCreatedEvent event = new OrderCreatedEvent(
                    eventId,
                    orderId,
                    request.customerId(),
                    request.productName(),
                    request.amount(),
                    "ORDER_CREATED"
            );

            System.out.println("[3] OrderCreatedEvent created");

            orderEventProducer.publish(event);

            OrderResponse response = new OrderResponse(orderId,
                    "ORDER_CREATED",
                    "new order created successfully");

            record.setOrderId(orderId);
            record.setStatus(IdempotencyStatus.COMPLETED);
            record.setResponseBody(writeResponse(response));
            idempotencyRepository.save(record);
            System.out.println("[IDEMPOTENCY] Request marked as COMPLETED");
            return response;
        } catch (Exception e) {
            record.setStatus(IdempotencyStatus.FAILED);
            record.setErrorMessage(e.getMessage());
            record.setUpdatedAt(LocalDateTime.now());
            idempotencyRepository.save(record);
            System.out.println("[IDEMPOTENCY] Request marked as FAILED");
            throw e;
        }
    }

    private String writeResponse(OrderResponse response) {
        return objectMapper.writeValueAsString(response);
    }

    private OrderResponse readResponse(String responseBody) {
        return objectMapper.readValue(responseBody, OrderResponse.class);
    }

}
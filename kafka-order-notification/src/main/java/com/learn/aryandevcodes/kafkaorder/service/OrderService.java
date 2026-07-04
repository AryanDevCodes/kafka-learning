package com.learn.aryandevcodes.kafkaorder.service;

import com.learn.aryandevcodes.kafkaorder.dto.OrderRequest;
import com.learn.aryandevcodes.kafkaorder.dto.OrderResponse;
import com.learn.aryandevcodes.kafkaorder.events.OrderCreatedEvent;
import com.learn.aryandevcodes.kafkaorder.producer.OrderEventProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderEventProducer orderEventProducer;
    private final OrderIdempotencyStorage orderIdempotencyStorage;

    public OrderResponse createOrder(OrderRequest request) {

        System.out.println("[1] Order creation started");

        if (orderIdempotencyStorage.isOrderAlreadyProcessed(request.clientRequestId())){
            System.out.println("[IDEMPOTENCY] Duplicate order request detected");
            System.out.println("[IDEMPOTENCY] Returning Previous response: " +
                    orderIdempotencyStorage.getPreviousResponse(request.clientRequestId()));
            return orderIdempotencyStorage.getPreviousResponse(request.clientRequestId());
        }

        String orderId = "ORD-" + UUID.randomUUID();
        String eventId = "EVT-" + UUID.randomUUID();

        System.out.println("[2] Order saved in database simulation");
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

        orderIdempotencyStorage.saveOrderResponse(request.clientRequestId(), response);
        System.out.println("[IDEMPOTENCY] Request saved as processed");
        return response;
    }

    /*
    public OrderResponse createDuplicateTestOrder(OrderRequest request) {

        System.out.println("[1] Duplicate test order started");

        String orderId = "ORD-DUPLICATE-TEST";
        String eventId = "EVT-DUPLICATE-TEST";

        OrderCreatedEvent event = new OrderCreatedEvent(
                eventId,
                orderId,
                request.customerId(),
                request.productName(),
                request.amount(),
                "ORDER_CREATED"
        );

        System.out.println("[2] Sending same event first time");
        orderEventProducer.publish(event);

        System.out.println("[3] Sending same event second time");
        orderEventProducer.publish(event);

        return new OrderResponse(orderId, "DUPLICATE_TEST_EVENT_SENT", "Duplicate test event sent successfully");
    }
    */
}
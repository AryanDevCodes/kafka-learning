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

    public OrderResponse createOrder(OrderRequest request) {

        System.out.println("[1] Order creation started");

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

        return new OrderResponse(orderId, "ORDER_CREATED");
    }
}
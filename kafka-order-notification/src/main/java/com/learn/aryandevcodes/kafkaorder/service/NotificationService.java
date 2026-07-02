package com.learn.aryandevcodes.kafkaorder.service;

import com.learn.aryandevcodes.kafkaorder.events.OrderCreatedEvent;
import org.springframework.stereotype.Service;

@Service
public class NotificationService {

    public void sendOrderNotification(OrderCreatedEvent event) {

        System.out.printf(
                """
                        
                        ================================
                        ORDER NOTIFICATION
                        ================================
                        Order created successfully.
                        Event ID: %s
                        Order ID: %s
                        Customer ID: %s
                        Product: %s
                        Amount: %.2f
                        Status: %s
                        ================================
                        %n""", event.eventId(),
        event.orderId(),
        event.customerId(),
        event.productName(),
        event.amount(),
        event.orderStatus()
);
    }
}
package com.learn.aryandevcodes.kafkaorder.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.learn.aryandevcodes.kafkaorder.events.OrderCreatedEvent;
import com.learn.aryandevcodes.kafkaorder.service.NotificationService;
import com.learn.aryandevcodes.kafkaorder.config.KafkaTopicConfig;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class OrderEventConsumer {
    
    private final NotificationService notificationService;

    @KafkaListener(
        topics = KafkaTopicConfig.ORDER_CREATED_TOPIC,
        groupId="order-notification-group"
    )
    public void consume(
        OrderCreatedEvent orderCreatedEvent
    ) {
        System.out.println("[CONSUMER] Event received from Kafka");
        System.out.println("[CONSUMER] Event ID: " + orderCreatedEvent.eventId());
        System.out.println("[CONSUMER] Order ID: " + orderCreatedEvent.orderId());

        notificationService.sendOrderNotification(orderCreatedEvent);
        }
}

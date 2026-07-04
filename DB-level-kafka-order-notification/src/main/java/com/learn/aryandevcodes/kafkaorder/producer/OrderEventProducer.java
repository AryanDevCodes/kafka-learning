package com.learn.aryandevcodes.kafkaorder.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.learn.aryandevcodes.kafkaorder.config.KafkaTopicConfig;
import com.learn.aryandevcodes.kafkaorder.events.OrderCreatedEvent;

import lombok.RequiredArgsConstructor;


@Component
@RequiredArgsConstructor
public class OrderEventProducer {
    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    public void publish(OrderCreatedEvent orderCreatedEvent) {
        System.out.println("[Producer] Sending order event to Kafka: " + orderCreatedEvent);
        System.out.println("[Producer] Topic:" + KafkaTopicConfig.ORDER_CREATED_TOPIC);
        System.out.println("[Producer] Key:" + orderCreatedEvent.orderId());
        kafkaTemplate.send(KafkaTopicConfig.ORDER_CREATED_TOPIC,
                            orderCreatedEvent.orderId(),
                            orderCreatedEvent)
                .whenComplete((result, error )->
                {
                    if (error != null) {
                        System.out.println("[Producer] Error sending order event to Kafka: "
                                + error.getClass().getSimpleName() + ": " + error.getMessage());
                        return;
                    }
                    System.out.println("[Producer] Sent successfully");
                    System.out.println("[Producer] Actual Partition: " + result.getRecordMetadata().partition());
                    System.out.println("[Producer] Offset: " + result.getRecordMetadata().offset());
                });
            System.out.println("[Producer] Send initiated (async)");
    }


}

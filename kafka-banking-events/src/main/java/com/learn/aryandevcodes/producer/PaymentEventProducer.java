package com.learn.aryandevcodes.producer;

import com.learn.aryandevcodes.events.PaymentCompletedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class PaymentEventProducer {

    private final KafkaTemplate<String, PaymentCompletedEvent> kafkaTemplate;

    @Value("${app.kafka.topic.payment-completed}")
    private String paymentCompletedTopic;

    public void publish(PaymentCompletedEvent event) {

        String key = event.userId().toString();

        System.out.println("[5] PRODUCER → Preparing to publish event to Kafka");
        System.out.println("    Topic         = " + paymentCompletedTopic);
        System.out.println("    Partition Key = " + key);
        System.out.println("    Reason        = Same userId events go to same partition");

        kafkaTemplate.send(paymentCompletedTopic, key, event)
                .whenComplete((result, exception) -> {

                    if (exception != null) {
                        System.out.println("[ERROR] PRODUCER → Failed to publish event");
                        System.out.println("    Error = " + exception.getMessage());
                        return;
                    }

                    System.out.println("[6] KAFKA → Event stored successfully");
                    System.out.println("    Topic     = " + result.getRecordMetadata().topic());
                    System.out.println("    Partition = " + result.getRecordMetadata().partition());
                    System.out.println("    Offset    = " + result.getRecordMetadata().offset());
                    System.out.println("    Timestamp = " + result.getRecordMetadata().timestamp());
                });
    }
}
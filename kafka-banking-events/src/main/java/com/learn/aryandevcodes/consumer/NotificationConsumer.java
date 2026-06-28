package com.learn.aryandevcodes.consumer;

import com.learn.aryandevcodes.events.PaymentCompletedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationConsumer {

    @KafkaListener(
            topics = "${app.kafka.topic.payment-completed}",
            groupId = "notification-group"
    )
    public void consume(PaymentCompletedEvent event) {

        System.out.println();
        System.out.println("[7] CONSUMER → NotificationConsumer received event from Kafka");
        System.out.println("    Event ID       = " + event.eventId());
        System.out.println("    Transaction ID = " + event.transactionId());
        System.out.println("    User ID        = " + event.userId());
        System.out.println("    Sender Account = " + event.senderAccountId());
        System.out.println("    Receiver Acc   = " + event.receiverAccountId());
        System.out.println("    Amount         = ₹" + event.amount());
        System.out.println("    Occurred At    = " + event.occurredAt());

        System.out.println("[8] NOTIFICATION → Simulating SMS notification");
        System.out.println("    SMS: Payment of ₹" + event.amount()
                + " completed successfully for transaction "
                + event.transactionId());

        System.out.println("[9] CONSUMER → Notification flow completed");
        System.out.println("========== PAYMENT EVENT FLOW COMPLETED ==========");
        System.out.println();
    }
}
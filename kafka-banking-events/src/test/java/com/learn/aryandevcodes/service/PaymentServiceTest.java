package com.learn.aryandevcodes.service;

import com.learn.aryandevcodes.dto.PaymentRequest;
import com.learn.aryandevcodes.events.PaymentCompletedEvent;
import com.learn.aryandevcodes.producer.PaymentEventProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class PaymentServiceTest {

    @Mock
    private PaymentEventProducer paymentEventProducer;

    @InjectMocks
    private PaymentService paymentService;

    @Test
    void testCompletePaymentIdempotency() {
        String requestId = UUID.randomUUID().toString();
        PaymentRequest request = new PaymentRequest(1L, 100L, 200L, new BigDecimal("100.00"), requestId);

        // First call
        PaymentCompletedEvent firstResponse = paymentService.completePayment(request);
        
        // Second call with same ID
        PaymentCompletedEvent secondResponse = paymentService.completePayment(request);

        assertEquals(firstResponse, secondResponse);
        verify(paymentEventProducer, times(1)).publish(any(PaymentCompletedEvent.class));
    }

    @Test
    void testCompletePaymentIdempotencyConcurrent() throws InterruptedException {
        String requestId = UUID.randomUUID().toString();
        PaymentRequest request = new PaymentRequest(1L, 100L, 200L, new BigDecimal("100.00"), requestId);
        
        int threads = 5;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        
        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    paymentService.completePayment(request);
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await();
        executor.shutdown();
        
        verify(paymentEventProducer, times(1)).publish(any(PaymentCompletedEvent.class));
    }
}

package com.learn.aryandevcodes.controller;

import com.learn.aryandevcodes.dto.PaymentRequest;
import com.learn.aryandevcodes.service.PaymentService;
import com.learn.aryandevcodes.events.PaymentCompletedEvent;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentService paymentService;

    @PostMapping("/complete")
    public PaymentCompletedEvent completePayment(@Valid @RequestBody PaymentRequest request) {

        System.out.println();
        System.out.println("========== PAYMENT EVENT FLOW STARTED ==========");
        System.out.println("[1] CLIENT → Sent POST request to /api/payments/complete");
        System.out.println("[2] CONTROLLER → PaymentController received request");
        System.out.println("    Request Body = " + request);

        PaymentCompletedEvent event = paymentService.completePayment(request);

        System.out.println("[9] CONTROLLER → Response returned to client");
        System.out.println("========== PAYMENT EVENT FLOW RESPONSE SENT ==========");
        System.out.println();

        return event;
    }
}
package com.learn.aryandevcodes.kafkaorder.controller;

import com.learn.aryandevcodes.kafkaorder.dto.OrderRequest;
import com.learn.aryandevcodes.kafkaorder.dto.OrderResponse;
import com.learn.aryandevcodes.kafkaorder.service.OrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    @PostMapping
    public OrderResponse createOrder(@Valid @RequestBody OrderRequest request) {

        System.out.println("[0] POST /orders called");

        return orderService.createOrder(request);
    }

    /*
    @PostMapping("/duplicate-test")
    public OrderResponse duplicateTest(@Valid @RequestBody OrderRequest request) {
        return orderService.createDuplicateTestOrder(request);
    }
    */
}
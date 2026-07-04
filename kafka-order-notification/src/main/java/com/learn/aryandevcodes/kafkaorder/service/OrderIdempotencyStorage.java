package com.learn.aryandevcodes.kafkaorder.service;

import com.learn.aryandevcodes.kafkaorder.dto.OrderResponse;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class OrderIdempotencyStorage {
    private final Map<String, OrderResponse> orderStorage = new ConcurrentHashMap<>();

    public boolean isOrderAlreadyProcessed(String clientRequestId) {
        return orderStorage.containsKey(clientRequestId);
    }

    public OrderResponse getPreviousResponse(String clientRequestId) {
        return orderStorage.get(clientRequestId);
    }

    public void saveOrderResponse(String clientRequestId, OrderResponse response) {
        orderStorage.put(clientRequestId, response);
    }
}

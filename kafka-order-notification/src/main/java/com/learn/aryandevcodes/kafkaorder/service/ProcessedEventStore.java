package com.learn.aryandevcodes.kafkaorder.service;

import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ProcessedEventStore {
    private final Set<String> processedEvents = ConcurrentHashMap.newKeySet();

    public boolean isEventProcessed(String eventId) {
        return processedEvents.contains(eventId);
    }

    public boolean markEventAsProcessed(String eventId) {
        return processedEvents.add(eventId);
    }
}

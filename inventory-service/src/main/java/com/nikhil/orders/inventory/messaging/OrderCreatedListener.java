package com.nikhil.orders.inventory.messaging;

import com.nikhil.orders.events.OrderCreatedEvent;
import com.nikhil.orders.events.Topics;
import com.nikhil.orders.inventory.domain.InventoryService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderCreatedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedListener.class);

    private final InventoryService inventoryService;
    private final long processingDelayMs;

    public OrderCreatedListener(
            InventoryService inventoryService,
            @Value("${demo.processing-delay-ms:0}") long processingDelayMs) {
        this.inventoryService = inventoryService;
        this.processingDelayMs = processingDelayMs;
    }

    @KafkaListener(topics = Topics.ORDER_EVENTS, groupId = "inventory-service")
    public void onOrderCreated(ConsumerRecord<String, OrderCreatedEvent> record) throws InterruptedException {
        OrderCreatedEvent event = record.value();
        log.info("Received OrderCreated order={} partition={} offset={}", event.orderId(), record.partition(), record.offset());

        boolean reserved = inventoryService.reserve(event);
        log.info("Reservation for order {} -> {}", event.orderId(), reserved ? "RESERVED" : "FAILED");

        if (processingDelayMs > 0) {
            Thread.sleep(processingDelayMs);
        }
    }
}

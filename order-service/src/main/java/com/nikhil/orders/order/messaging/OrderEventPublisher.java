package com.nikhil.orders.order.messaging;

import com.nikhil.orders.events.OrderCreatedEvent;
import com.nikhil.orders.events.Topics;
import com.nikhil.orders.order.domain.Order;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(OrderEventPublisher.class);

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    public OrderEventPublisher(KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishOrderCreated(Order order) {
        OrderCreatedEvent event = new OrderCreatedEvent(
                UUID.randomUUID(),
                order.getId(),
                order.getCustomerId(),
                order.getProductId(),
                order.getQuantity(),
                order.getTotalAmount(),
                Instant.now());

        kafkaTemplate.send(Topics.ORDER_EVENTS, order.getId().toString(), event)
                .whenComplete((result, error) -> {
                    if (error != null) {
                        log.error("Failed to publish OrderCreated for order {}", order.getId(), error);
                    } else {
                        log.info("Published OrderCreated for order {} to partition {} offset {}",
                                order.getId(), result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                    }
                });
    }
}

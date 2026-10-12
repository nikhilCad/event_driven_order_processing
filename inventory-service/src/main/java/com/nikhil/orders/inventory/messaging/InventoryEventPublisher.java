package com.nikhil.orders.inventory.messaging;

import com.nikhil.orders.events.Topics;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public InventoryEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(UUID orderId, Object event) {
        kafkaTemplate.send(Topics.INVENTORY_EVENTS, orderId.toString(), event)
                .whenComplete((result, error) -> {
                    if (error != null) {
                        log.error("Failed to publish {} for order {}", event.getClass().getSimpleName(), orderId, error);
                    } else {
                        log.info("Published {} for order {} to partition {} offset {}",
                                event.getClass().getSimpleName(), orderId,
                                result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                    }
                });
    }
}

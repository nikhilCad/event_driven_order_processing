package com.nikhil.orders.order.messaging;

import com.nikhil.orders.events.OrderCreatedEvent;
import com.nikhil.orders.events.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderCreatedLogger {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedLogger.class);

    @KafkaListener(topics = Topics.ORDER_EVENTS, groupId = "order-debug-logger")
    public void onOrderCreated(ConsumerRecord<String, OrderCreatedEvent> record) {
        log.info("Consumed key={} partition={} offset={} event={}",
                record.key(), record.partition(), record.offset(), record.value());
    }
}

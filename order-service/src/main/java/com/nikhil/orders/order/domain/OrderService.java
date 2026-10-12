package com.nikhil.orders.order.domain;

import com.nikhil.orders.order.messaging.OrderEventPublisher;
import java.math.BigDecimal;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderEventPublisher eventPublisher;
    private final boolean failAfterPublish;
    private final boolean publishAfterCommit;

    public OrderService(
            OrderRepository orderRepository,
            OrderEventPublisher eventPublisher,
            @Value("${demo.fail-after-publish:false}") boolean failAfterPublish,
            @Value("${demo.publish-after-commit:false}") boolean publishAfterCommit) {
        this.orderRepository = orderRepository;
        this.eventPublisher = eventPublisher;
        this.failAfterPublish = failAfterPublish;
        this.publishAfterCommit = publishAfterCommit;
    }

    @Transactional
    public Order createOrder(String customerId, String productId, int quantity, BigDecimal totalAmount) {
        Order order = orderRepository.save(Order.create(customerId, productId, quantity, totalAmount));
        if (publishAfterCommit) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    eventPublisher.publishOrderCreated(order);
                }
            });
        } else {
            eventPublisher.publishOrderCreated(order);
        }
        if (failAfterPublish) {
            throw new IllegalStateException("simulated failure after publish, before commit");
        }
        return order;
    }

    @Transactional(readOnly = true)
    public Order getOrder(UUID id) {
        return orderRepository.findById(id).orElseThrow(() -> new OrderNotFoundException(id));
    }
}

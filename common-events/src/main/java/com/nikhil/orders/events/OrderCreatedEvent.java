package com.nikhil.orders.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderCreatedEvent(
        UUID eventId,
        UUID orderId,
        String customerId,
        String productId,
        int quantity,
        BigDecimal totalAmount,
        Instant occurredAt) {
}

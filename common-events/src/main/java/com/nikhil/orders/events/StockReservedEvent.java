package com.nikhil.orders.events;

import java.time.Instant;
import java.util.UUID;

public record StockReservedEvent(
        UUID eventId,
        UUID orderId,
        String productId,
        int quantity,
        Instant occurredAt) {
}

package com.nikhil.orders.events;

import java.time.Instant;
import java.util.UUID;

public record StockReservationFailedEvent(
        UUID eventId,
        UUID orderId,
        String productId,
        int quantity,
        String reason,
        Instant occurredAt) {
}

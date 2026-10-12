package com.nikhil.orders.inventory.domain;

import com.nikhil.orders.events.OrderCreatedEvent;
import com.nikhil.orders.events.StockReservationFailedEvent;
import com.nikhil.orders.events.StockReservedEvent;
import com.nikhil.orders.inventory.messaging.InventoryEventPublisher;
import java.time.Instant;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class InventoryService {

    private final StockRepository stockRepository;
    private final ReservationRepository reservationRepository;
    private final InventoryEventPublisher eventPublisher;

    public InventoryService(
            StockRepository stockRepository,
            ReservationRepository reservationRepository,
            InventoryEventPublisher eventPublisher) {
        this.stockRepository = stockRepository;
        this.reservationRepository = reservationRepository;
        this.eventPublisher = eventPublisher;
    }

    @Transactional
    public boolean reserve(OrderCreatedEvent event) {
        int updated = stockRepository.reserve(event.productId(), event.quantity());

        if (updated == 1) {
            reservationRepository.save(Reservation.reserved(event.orderId(), event.productId(), event.quantity()));
            eventPublisher.publish(event.orderId(), new StockReservedEvent(
                    UUID.randomUUID(), event.orderId(), event.productId(), event.quantity(), Instant.now()));
            return true;
        }

        String reason = stockRepository.existsById(event.productId()) ? "INSUFFICIENT_STOCK" : "UNKNOWN_PRODUCT";
        eventPublisher.publish(event.orderId(), new StockReservationFailedEvent(
                UUID.randomUUID(), event.orderId(), event.productId(), event.quantity(), reason, Instant.now()));
        return false;
    }

    @Transactional
    public Stock setStock(String productId, int available) {
        return stockRepository.save(new Stock(productId, available));
    }

    @Transactional(readOnly = true)
    public int availableFor(String productId) {
        return stockRepository.findById(productId).map(Stock::getAvailable).orElse(0);
    }
}

package com.nikhil.orders.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import com.nikhil.orders.events.OrderCreatedEvent;
import com.nikhil.orders.events.Topics;
import com.nikhil.orders.inventory.domain.InventoryService;
import com.nikhil.orders.inventory.domain.ReservationRepository;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

@SpringBootTest
@Testcontainers
class InventoryServiceIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16.4");

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:4.0.0");

    @Autowired
    InventoryService inventoryService;

    @Autowired
    ReservationRepository reservationRepository;

    @Test
    void concurrentReservationsNeverOversell() throws Exception {
        inventoryService.setStock("p-race", 10);
        int attempts = 50;
        ExecutorService pool = Executors.newFixedThreadPool(attempts);
        CountDownLatch startGate = new CountDownLatch(1);

        List<Future<Boolean>> results = new ArrayList<>();
        for (int i = 0; i < attempts; i++) {
            results.add(pool.submit(() -> {
                startGate.await();
                return inventoryService.reserve(orderCreated("p-race", 1));
            }));
        }
        startGate.countDown();

        long reserved = 0;
        for (Future<Boolean> result : results) {
            if (result.get()) {
                reserved++;
            }
        }
        pool.shutdown();

        assertThat(reserved).isEqualTo(10);
        assertThat(inventoryService.availableFor("p-race")).isZero();
        assertThat(reservationRepository.findAll().stream().filter(r -> r.getProductId().equals("p-race")))
                .hasSize(10);
    }

    @Test
    void orderCreatedEventsProduceReservedOrFailedEventsKeyedByOrderId() throws Exception {
        inventoryService.setStock("p-e2e", 5);
        UUID reservedOrder = UUID.randomUUID();
        UUID unknownProductOrder = UUID.randomUUID();
        UUID tooManyOrder = UUID.randomUUID();

        sendOrderCreated(reservedOrder, "p-e2e", 2);
        sendOrderCreated(unknownProductOrder, "p-does-not-exist", 1);
        sendOrderCreated(tooManyOrder, "p-e2e", 99);

        Map<String, String> events = awaitEvents(List.of(reservedOrder, unknownProductOrder, tooManyOrder));

        assertThat(events.get(reservedOrder.toString())).contains("\"quantity\":2").doesNotContain("reason");
        assertThat(events.get(unknownProductOrder.toString())).contains("UNKNOWN_PRODUCT");
        assertThat(events.get(tooManyOrder.toString())).contains("INSUFFICIENT_STOCK");
        assertThat(awaitAvailable("p-e2e", 3)).isEqualTo(3);
    }

    private static OrderCreatedEvent orderCreated(String productId, int quantity) {
        return new OrderCreatedEvent(
                UUID.randomUUID(), UUID.randomUUID(), "c-1", productId, quantity, BigDecimal.TEN, Instant.now());
    }

    private void sendOrderCreated(UUID orderId, String productId, int quantity) throws Exception {
        String json = """
                {"eventId":"%s","orderId":"%s","customerId":"c-1","productId":"%s","quantity":%d,\
                "totalAmount":10.00,"occurredAt":"2026-10-12T00:00:00Z"}
                """.formatted(UUID.randomUUID(), orderId, productId, quantity);
        Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(config, new StringSerializer(), new StringSerializer())) {
            producer.send(new ProducerRecord<>(Topics.ORDER_EVENTS, orderId.toString(), json)).get();
        }
    }

    private Map<String, String> awaitEvents(List<UUID> orderIds) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Map<String, String> found = new HashMap<>();
        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(Topics.INVENTORY_EVENTS));
            long deadline = System.currentTimeMillis() + 20_000;
            while (System.currentTimeMillis() < deadline && found.size() < orderIds.size()) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (orderIds.stream().anyMatch(id -> id.toString().equals(record.key()))) {
                        found.put(record.key(), record.value());
                    }
                }
            }
        }
        assertThat(found).hasSize(orderIds.size());
        return found;
    }

    private int awaitAvailable(String productId, int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        int available = inventoryService.availableFor(productId);
        while (available != expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
            available = inventoryService.availableFor(productId);
        }
        return available;
    }

}

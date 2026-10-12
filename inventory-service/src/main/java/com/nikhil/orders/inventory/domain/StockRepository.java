package com.nikhil.orders.inventory.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface StockRepository extends JpaRepository<Stock, String> {

    @Modifying(clearAutomatically = true)
    @Query("update Stock s set s.available = s.available - :quantity "
            + "where s.productId = :productId and s.available >= :quantity")
    int reserve(@Param("productId") String productId, @Param("quantity") int quantity);
}

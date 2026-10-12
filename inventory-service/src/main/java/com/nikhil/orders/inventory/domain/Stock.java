package com.nikhil.orders.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "stock")
public class Stock {

    @Id
    @Column(name = "product_id")
    private String productId;

    @Column(nullable = false)
    private int available;

    protected Stock() {
    }

    public Stock(String productId, int available) {
        this.productId = productId;
        this.available = available;
    }

    public String getProductId() {
        return productId;
    }

    public int getAvailable() {
        return available;
    }
}

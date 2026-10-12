package com.nikhil.orders.inventory.web;

import jakarta.validation.constraints.Min;

public record SetStockRequest(@Min(0) int available) {
}

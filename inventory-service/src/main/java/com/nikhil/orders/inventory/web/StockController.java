package com.nikhil.orders.inventory.web;

import com.nikhil.orders.inventory.domain.InventoryService;
import com.nikhil.orders.inventory.domain.Stock;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/stock")
public class StockController {

    private final InventoryService inventoryService;

    public StockController(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @PutMapping("/{productId}")
    public StockResponse set(@PathVariable String productId, @Valid @RequestBody SetStockRequest request) {
        Stock stock = inventoryService.setStock(productId, request.available());
        return new StockResponse(stock.getProductId(), stock.getAvailable());
    }

    @GetMapping("/{productId}")
    public StockResponse get(@PathVariable String productId) {
        return new StockResponse(productId, inventoryService.availableFor(productId));
    }
}

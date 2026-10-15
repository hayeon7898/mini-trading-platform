package com.mts.orderserver.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;

@Entity
@Table(name = "stocks")
public class Stock {

    @Id
    @Column(length = 12)
    private String symbol;

    @Column(nullable = false, length = 100)
    private String name;

    @Column(nullable = false, length = 10)
    private String market;

    @Column(name = "base_price", nullable = false, precision = 18, scale = 4)
    private BigDecimal basePrice;

    protected Stock() {
    }

    public String getSymbol() { return symbol; }
    public String getName() { return name; }
    public String getMarket() { return market; }
    public BigDecimal getBasePrice() { return basePrice; }
}

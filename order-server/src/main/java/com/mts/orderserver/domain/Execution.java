package com.mts.orderserver.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "executions")
public class Execution {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_id", nullable = false)
    private Long orderId;

    @Column(name = "account_id", nullable = false)
    private Long accountId;

    @Column(nullable = false, length = 12)
    private String symbol;

    @Enumerated(EnumType.STRING)
    @JdbcTypeCode(SqlTypes.VARCHAR)
    @Column(nullable = false, length = 4)
    private OrderSide side;

    @Column(nullable = false, precision = 18, scale = 4)
    private BigDecimal price;

    @Column(nullable = false)
    private int quantity;

    /** UTC */
    @Column(name = "executed_at", nullable = false)
    private LocalDateTime executedAt;

    protected Execution() {
    }

    /**
     * 주문에서 계좌·종목·방향을 복사해 체결 기록을 만든다.
     * 외부에서는 Order.fill()을 통해서만 만들도록 package-private으로 둔다.
     */
    static Execution of(Order order, int quantity, BigDecimal price, LocalDateTime executedAt) {
        Execution e = new Execution();
        e.orderId = order.getId();
        e.accountId = order.getAccountId();
        e.symbol = order.getSymbol();
        e.side = order.getSide();
        e.price = price;
        e.quantity = quantity;
        e.executedAt = executedAt;
        return e;
    }

    public Long getId() { return id; }
    public Long getOrderId() { return orderId; }
    public Long getAccountId() { return accountId; }
    public String getSymbol() { return symbol; }
    public OrderSide getSide() { return side; }
    public BigDecimal getPrice() { return price; }
    public int getQuantity() { return quantity; }
    public LocalDateTime getExecutedAt() { return executedAt; }
}
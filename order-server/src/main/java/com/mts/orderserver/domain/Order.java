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
import java.util.Objects;

@Entity
@Table(name = "orders")
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "account_id", nullable = false)
    private Long accountId;

    @Column(nullable = false, length = 12)
    private String symbol;

    // MySQL 방언은 enum을 네이티브 ENUM 컬럼으로 기대한다. DDL은 VARCHAR라 명시한다.
    @Enumerated(EnumType.STRING)
    @JdbcTypeCode(SqlTypes.VARCHAR)
    @Column(nullable = false, length = 4)
    private OrderSide side;

    @Column(nullable = false, precision = 18, scale = 4)
    private BigDecimal price;

    @Column(nullable = false)
    private int quantity;

    @Column(name = "filled_qty", nullable = false)
    private int filledQty;

    @Enumerated(EnumType.STRING)
    @JdbcTypeCode(SqlTypes.VARCHAR)
    @Column(nullable = false, length = 16)
    private OrderStatus status;

    /** UTC */
    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    /** UTC */
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    protected Order() {
    }

    /**
     * 접수 상태(ACCEPTED)의 새 주문.
     * 요청 형식 검증은 API 계층에서 하고, 여기서는 도메인 불변식만 지킨다.
     *
     * @param now UTC 시각 (Clock에서 만든 값)
     */
    public static Order accept(Long accountId, String symbol, OrderSide side,
                               BigDecimal price, int quantity, LocalDateTime now) {
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(symbol, "symbol");
        Objects.requireNonNull(side, "side");
        Objects.requireNonNull(price, "price");
        Objects.requireNonNull(now, "now");
        if (price.signum() <= 0) {
            throw new IllegalArgumentException("price must be positive: " + price);
        }
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be positive: " + quantity);
        }

        Order order = new Order();
        order.accountId = accountId;
        order.symbol = symbol;
        order.side = side;
        order.price = price;
        order.quantity = quantity;
        order.filledQty = 0;
        order.status = OrderStatus.ACCEPTED;
        order.createdAt = now;
        order.updatedAt = now;
        return order;
    }

    /**
     * 미체결 잔량을 취소한다. 이미 체결된 수량(filledQty)은 그대로 남는다.
     * 동시 체결과의 경합은 호출하는 쪽이 행 락을 잡아 막는다.
     *
     * @param now UTC 시각 (Clock에서 만든 값)
     * @throws OrderNotCancelableException FILLED 또는 CANCELED 상태일 때
     */
    public void cancel(LocalDateTime now) {
        if (!isOpen()) {
            throw new OrderNotCancelableException(id, status);
        }
        this.status = OrderStatus.CANCELED;
        this.updatedAt = now;
    }

    /** 미체결 잔량이 남아 체결·취소 대상이 되는 상태인가. */
    public boolean isOpen() {
        return status == OrderStatus.ACCEPTED || status == OrderStatus.PARTIAL;
    }

    public Long getId() { return id; }
    public Long getAccountId() { return accountId; }
    public String getSymbol() { return symbol; }
    public OrderSide getSide() { return side; }
    public BigDecimal getPrice() { return price; }
    public int getQuantity() { return quantity; }
    public int getFilledQty() { return filledQty; }
    public OrderStatus getStatus() { return status; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public LocalDateTime getUpdatedAt() { return updatedAt; }
}
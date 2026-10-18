package com.mts.orderserver.api;

import com.mts.orderserver.api.dto.CreateOrderRequest;
import com.mts.orderserver.api.dto.OrderResponse;
import com.mts.orderserver.domain.Order;
import com.mts.orderserver.service.OrderService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<OrderResponse> place(@Valid @RequestBody CreateOrderRequest request) {
        Order order = orderService.place(
                request.accountId(), request.symbol(), request.side(),
                request.price(), request.quantity());
        return ResponseEntity
                .created(URI.create("/orders/" + order.getId()))
                .body(OrderResponse.from(order));
    }

    @GetMapping("/{id}")
    public OrderResponse get(@PathVariable Long id) {
        return OrderResponse.from(orderService.get(id));
    }

    /** 미체결 잔량 취소. 잔량이 없으면(FILLED, CANCELED) 409. */
    @PostMapping("/{id}/cancel")
    public OrderResponse cancel(@PathVariable Long id) {
        return OrderResponse.from(orderService.cancel(id));
    }
}
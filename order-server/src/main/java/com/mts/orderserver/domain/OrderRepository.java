package com.mts.orderserver.domain;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;

public interface OrderRepository extends JpaRepository<Order, Long> {

    /**
     * SELECT ... FOR UPDATE. 트랜잭션이 끝날 때까지 행을 잠근다.
     * 취소와 체결 시뮬레이터가 같은 주문을 동시에 바꾸지 못하게 한다.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select o from Order o where o.id = :id")
    Optional<Order> findByIdForUpdate(@Param("id") Long id);

    /**
     * 체결 대상(잔량이 남은 주문)을 최대 limit건 잠가서 가져온다.
     *
     * - SKIP LOCKED: 다른 워커나 취소 요청이 잠근 행은 기다리지 않고 건너뛴다.
     * - ORDER BY status, id: idx_orders_status_id 순서와 같아 filesort 없이 LIMIT 행만 읽고 잠근다.
     *   ORDER BY id로 하면 열린 주문 전체를 정렬하거나 PK를 처음부터 훑어 훨씬 많은 행을 건드린다.
     * - ACCEPTED가 PARTIAL보다 먼저 처리된다 (밀린 주문이 많으면 PARTIAL이 늦어질 수 있음).
     */
    @Query(value = """
            SELECT * FROM orders
            WHERE status IN ('ACCEPTED', 'PARTIAL')
            ORDER BY status, id
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<Order> lockFillable(@Param("limit") int limit);
}
package com.mts.orderserver.domain;

import jakarta.persistence.LockModeType;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
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

    /*
     * 계좌별 주문 내역.
     * - 상태 필터 유무로 메서드를 나눈다. (:status IS NULL OR ...) 한 쿼리로 합치면
     *   옵티마이저가 인덱스를 제대로 못 쓴다.
     * - 반환 타입이 List라 Pageable을 줘도 COUNT 쿼리가 나가지 않는다 (LIMIT/OFFSET만 적용).
     * - 기간은 [from, to) 반열린 구간. created_at이 같으면 id로 순서를 고정한다.
     */

    /** OFFSET 방식 (Pageable의 page로 건너뛸 행 수 지정). */
    @Query("""
            select o from Order o
            where o.accountId = :accountId
              and o.createdAt >= :from and o.createdAt < :to
            order by o.createdAt desc, o.id desc
            """)
    List<Order> findAccountOrders(@Param("accountId") Long accountId,
                                  @Param("from") LocalDateTime from,
                                  @Param("to") LocalDateTime to,
                                  Pageable pageable);

    @Query("""
            select o from Order o
            where o.accountId = :accountId
              and o.status = :status
              and o.createdAt >= :from and o.createdAt < :to
            order by o.createdAt desc, o.id desc
            """)
    List<Order> findAccountOrdersByStatus(@Param("accountId") Long accountId,
                                          @Param("status") OrderStatus status,
                                          @Param("from") LocalDateTime from,
                                          @Param("to") LocalDateTime to,
                                          Pageable pageable);

    /*
     * 커서 방식: (created_at, id) < (cursorAt, cursorId) 인 행부터.
     * created_at <= cursorAt를 따로 두어 인덱스 범위의 상한이 되게 하고,
     * 같은 시각이면 id로 이어지게 한다. 건너뛸 행을 읽지 않으므로 깊이와 무관하게 비용이 일정하다.
     */

    @Query("""
            select o from Order o
            where o.accountId = :accountId
              and o.createdAt >= :from and o.createdAt < :to
              and o.createdAt <= :cursorAt
              and (o.createdAt < :cursorAt or o.id < :cursorId)
            order by o.createdAt desc, o.id desc
            """)
    List<Order> findAccountOrdersAfter(@Param("accountId") Long accountId,
                                       @Param("from") LocalDateTime from,
                                       @Param("to") LocalDateTime to,
                                       @Param("cursorAt") LocalDateTime cursorAt,
                                       @Param("cursorId") Long cursorId,
                                       Pageable pageable);

    @Query("""
            select o from Order o
            where o.accountId = :accountId
              and o.status = :status
              and o.createdAt >= :from and o.createdAt < :to
              and o.createdAt <= :cursorAt
              and (o.createdAt < :cursorAt or o.id < :cursorId)
            order by o.createdAt desc, o.id desc
            """)
    List<Order> findAccountOrdersByStatusAfter(@Param("accountId") Long accountId,
                                               @Param("status") OrderStatus status,
                                               @Param("from") LocalDateTime from,
                                               @Param("to") LocalDateTime to,
                                               @Param("cursorAt") LocalDateTime cursorAt,
                                               @Param("cursorId") Long cursorId,
                                               Pageable pageable);
}
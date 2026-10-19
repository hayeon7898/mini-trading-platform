package com.mts.orderserver.domain;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;

public interface ExecutionRepository extends JpaRepository<Execution, Long> {

    /** 계좌별 체결 내역. 기간은 [from, to), COUNT 쿼리 없음. */
    @Query("""
            select e from Execution e
            where e.accountId = :accountId
              and e.executedAt >= :from and e.executedAt < :to
            order by e.executedAt desc, e.id desc
            """)
    List<Execution> findAccountExecutions(@Param("accountId") Long accountId,
                                          @Param("from") LocalDateTime from,
                                          @Param("to") LocalDateTime to,
                                          Pageable pageable);
}
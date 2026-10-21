-- 종목별 1분 단위 거래대금·체결 수 (최근 1시간)
SELECT
    toStartOfMinute(executed_at) AS minute,
    symbol,
    count()                      AS executions,
    sum(price * quantity)        AS amount
FROM mts.executions FINAL
WHERE executed_at >= now64(3) - INTERVAL 1 HOUR
GROUP BY minute, symbol
ORDER BY minute DESC, amount DESC
LIMIT 50;

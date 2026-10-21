-- 최근 10분 거래대금 상위 10개 종목
SELECT
    symbol,
    count()               AS executions,
    sum(quantity)         AS volume,
    sum(price * quantity) AS amount
FROM mts.executions FINAL
WHERE executed_at >= now64(3) - INTERVAL 10 MINUTE
GROUP BY symbol
ORDER BY amount DESC
LIMIT 10;

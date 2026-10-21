-- 매수/매도 체결 수량 비율 추이 (1분 단위, 최근 1시간)
SELECT
    toStartOfMinute(executed_at)                           AS minute,
    sumIf(quantity, side = 'BUY')                          AS buy_volume,
    sumIf(quantity, side = 'SELL')                         AS sell_volume,
    round(buy_volume / nullIf(buy_volume + sell_volume, 0), 3) AS buy_ratio
FROM mts.executions FINAL
WHERE executed_at >= now64(3) - INTERVAL 1 HOUR
GROUP BY minute
ORDER BY minute;

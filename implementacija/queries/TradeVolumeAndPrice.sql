-- Odrediti kako su periodi sa znatno velikim obimom trgovine uticali na promenu cene izabranog para kriptovaluta
WITH LargeTrades AS (
    SELECT 
        mc.open_time,
        mc.pairid,
        mc.volume,
        mc.close,
        LEAD(mc.close, 1) OVER (PARTITION BY mc.pairid ORDER BY mc.open_time) AS next_minute_close
    FROM 
        minute_candlesticks mc
    JOIN 
        daily_trading_info dt
    ON 
        to_date(mc.open_time) = dt.`date` AND
        mc.pairid = dt.pairid
    WHERE 
        mc.volume > dt.avg_trade_volume * 2
),
PriceImpact AS (
    SELECT 
        lt.open_time,
        lt.pairid,
        lt.volume,
        lt.close,
        lt.next_minute_close,
        (lt.next_minute_close - lt.close) / lt.close AS price_change_percentage
    FROM 
        LargeTrades lt
)
SELECT 
    open_time, 
    pairid, 
    volume, 
    price_change_percentage 
FROM 
    PriceImpact
WHERE 
    to_date(open_time) > CAST('2019-01-01' AS DATE);

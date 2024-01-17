WITH RankedPeriods AS (
    SELECT 
        `date`, 
        AVG(rsi) OVER (ORDER BY `date` RANGE BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_rsi
    FROM 
        technical_analysis
    WHERE pairid = 'BTC-USDT' AND `date` >= '2020-01-01'
)
SELECT
    `date`, 
    avg_rsi
FROM 
    RankedPeriods
ORDER BY 
    avg_rsi ASC
LIMIT 5;
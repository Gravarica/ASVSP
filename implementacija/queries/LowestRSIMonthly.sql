WITH MonthlyAvgRSI AS (
    SELECT 
        YEAR(`date`) as year,
        MONTH(`date`) as month,
        pairid,
        AVG(rsi) as avg_rsi
    FROM 
        technical_analysis
    WHERE 
        pairid = 'BTC-USDT'
    GROUP BY 
        YEAR(`date`), MONTH(`date`), pairid
)
SELECT 
    year,
    month,
    avg_rsi
FROM 
    MonthlyAvgRSI
ORDER BY 
    avg_rsi ASC
LIMIT 5;
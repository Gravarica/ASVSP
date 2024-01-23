-- Izracunati 30-dnevni pokretni prosek obima razmene za svaki par kriptovaluta i odrediti da li je trend rastuci ili opadajuci

WITH MovingAverages AS (
    SELECT
        pairid,
        `date`,
        total_trade_volume,
        AVG(total_trade_volume) OVER (PARTITION BY pairid ORDER BY `date` ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS MovingAvgVolume
    FROM 
        btc_usdt_daily_averages
),
VolumeTrend AS (
    SELECT
        pairid,
        `date`,
        total_trade_volume,
        MovingAvgVolume,
        LAG(MovingAvgVolume, 1) OVER (PARTITION BY pairid ORDER BY `date`) AS PrevMovingAvgVolume
    FROM
        MovingAverages
)
SELECT 
    pairid,
    `date`,
    total_trade_volume,
    MovingAvgVolume,
    CASE 
        WHEN MovingAvgVolume > PrevMovingAvgVolume THEN 'Increasing'
        ELSE 'Not Increasing'
    END AS VolumeTrend
FROM 
    VolumeTrend
ORDER BY 
    pairid, 
    `date`;

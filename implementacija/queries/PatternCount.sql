-- Prebrojati koliko puta se pojavio Hammer ili ShootingStar obrazac za BTC-USDT par 

WITH CandlestickPatterns AS (
    SELECT
        pairid,
        CAST(open_time AS DATE) AS `date`,
        open,
        close,
        high,
        low,
        ABS(close - open) AS BodySize,
        CASE
            WHEN (ABS(close - open) <= (LEAST(open, close) - low) / 2 AND (LEAST(open, close) - low) >= 5 * ABS(close - open) AND (high - GREATEST(open, close)) <= ABS(close - open)) THEN 'Hammer'
            WHEN (ABS(close - open) <= (high - GREATEST(open, close)) / 2 AND (high - GREATEST(open, close)) >= 5 * ABS(close - open) AND (LEAST(open, close) - low)<= ABS(close - open)) THEN 'Shooting Star'
            ELSE 'None'
        END AS Pattern
    FROM
        minute_candlesticks
    WHERE
        pairid = 'BTC-USDT'
),
MonthlyPatternCount AS (
    SELECT
        pairid,
        YEAR(`date`) AS year,
        MONTH(`date`) AS month,
        COUNT(CASE WHEN Pattern = 'Hammer' THEN 1 ELSE NULL END) AS HammerCount,
        COUNT(CASE WHEN Pattern = 'Shooting Star' THEN 1 ELSE NULL END) AS ShootingStarCount
    FROM
        CandlestickPatterns
    GROUP BY
        pairid,
        YEAR(`date`),
        MONTH(`date`)
)
SELECT *
FROM MonthlyPatternCount
WHERE year >= 2018
ORDER BY year, month;

-- Superset

WITH CandlestickPatterns AS (
    SELECT
        pairid,
        CAST(open_time AS DATE) AS `date`,
        open,
        close,
        high,
        low,
        ABS(close - open) AS BodySize,
        CASE
            WHEN (ABS(close - open) <= (LEAST(open, close) - low) / 2 AND (LEAST(open, close) - low) >= 5 * ABS(close - open) AND (high - GREATEST(open, close)) <= ABS(close - open)) THEN 'Hammer'
            WHEN (ABS(close - open) <= (high - GREATEST(open, close)) / 2 AND (high - GREATEST(open, close)) >= 5 * ABS(close - open) AND (LEAST(open, close) - low)<= ABS(close - open)) THEN 'Shooting Star'
            ELSE 'None'
        END AS Pattern
    FROM
        minute_candlesticks
    WHERE
        pairid = 'BTC-USDT'
),
MonthlyPatternCount AS (
    SELECT
        pairid,
        YEAR(`date`) AS year,
        MONTH(`date`) AS month,
        COUNT(CASE WHEN Pattern = 'Hammer' THEN 1 ELSE NULL END) AS HammerCount,
        COUNT(CASE WHEN Pattern = 'Shooting Star' THEN 1 ELSE NULL END) AS ShootingStarCount
    FROM
        CandlestickPatterns
    GROUP BY
        pairid,
        YEAR(`date`),
        MONTH(`date`)
),
FormattedMonthlyPatternCount AS (
    SELECT
        pairid,
        CAST(CONCAT(year, '-', LPAD(month, 2, '0'), '-01') AS DATE) AS MonthStart,
        HammerCount,
        ShootingStarCount
    FROM
        MonthlyPatternCount
    WHERE
        year >= 2018
)
SELECT *
FROM FormattedMonthlyPatternCount
ORDER BY MonthStart;


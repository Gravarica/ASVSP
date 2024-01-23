-- Za svaki par valuta odrediti najvecu i najmanju cenu tokom svake godine

SELECT YEAR(`date`) AS year,
       PairID,
       MAX(dailyhigh) AS yearly_high,
       MIN(dailylow) AS yearly_low
FROM daily_candlesticks
GROUP BY YEAR(`date`), PairID
ORDER BY year, PairID;
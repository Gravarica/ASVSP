-- Odrediti uticaj visokog/niskog RSI indeksa na promenu u ceni i obimu prodaje odredjene kriptovalute

SELECT 
    dc.`date`, 
    dc.pairid,
    ti.RSI,
    dc.dailyclose,
    LEAD(dc.dailyclose) OVER (PARTITION BY dc.PairID ORDER BY dc.`date`) AS NextDayClosing,
    dc.dailyvolume,
    LEAD(dc.dailyvolume) OVER (PARTITION BY dc.PairID ORDER BY dc.`date`) AS NextDayVolume
FROM 
    daily_candlesticks dc
JOIN 
    technical_indicators ti ON dc.PairID = ti.PairID AND dc.`date` = ti.`date`
WHERE 
    ti.RSI > 70 AND YEAR(dc.`date`) >= 2020
    
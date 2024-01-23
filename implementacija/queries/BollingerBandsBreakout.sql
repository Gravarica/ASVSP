-- Za izabranu kriptovalutu odrediti periode kada je cena zatvaranja bila iznad ili ispod bolingerovog pojasa

SELECT t.`date`, t.PairID, c.dailyclose, t.lowerband, t.upperband
FROM daily_candlesticks c
JOIN technical_indicators t ON (c.PairID = t.PairID AND c.`date` = t.`date`)
WHERE c.dailyclose > t.upperBand OR c.dailyclose < t.lowerBand;
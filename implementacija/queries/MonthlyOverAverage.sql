WITH monthly_avg_trades AS
(SELECT 
    PairID, 
    YEAR(`date`) AS Year, 
    MONTH(`date`) AS Month, 
    AVG(total_num_trades) AS avg_mon_trades
FROM 
    btc_usdt_daily_averages
GROUP BY 
    PairID, 
    YEAR(`date`), 
    MONTH(`date`))
SELECT a.PairID, `date`, a.total_num_trades, b.avg_mon_trades
FROM 
    btc_usdt_daily_averages a
JOIN 
    monthly_avg_trades b
ON
    a.PairID = b.PairID AND 
    YEAR(`date`) = b.Year AND 
    MONTH(`date`) = b.Month
WHERE 
    a.PairID = 'BTC-USDT' AND
    a.total_num_trades > b.avg_mon_trades * 1.5;
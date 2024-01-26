-- Za svaku godinu sracunati ukupan promet na trzistu izrazen u milionima dolara

SELECT 
    YEAR(`date`) AS year,
    SUM(total_quote_asset_volume) / 1000000 AS TotalVolumeInMillions
FROM 
    daily_trading_info
GROUP BY 
    YEAR(`date`)
ORDER BY 
    year;
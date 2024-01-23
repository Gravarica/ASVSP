-- Za svaki par kriptovaluta odrediti promenu u koeficijentu korelacije sa drugim parovima na kvartalnom nivou u toku godine 

WITH QuarterAverages AS (
SELECT DISTINCT
    Year,
    CEIL(Month / 3) AS Quarter,
    PairID1,
    PairID2,
    AVG(Correlation) OVER (PARTITION BY Year, CEIL(Month / 3), PairID1, PairID2) AS AvgQuarterCorrelation
FROM
    MonthlyCorrelation
WHERE year > 2018
ORDER BY year, quarter, pairid1, pairid2
),
QuarterChanges AS (
SELECT
        Year, 
        Quarter, 
        PairID1, 
        PairID2, 
        AvgQuarterCorrelation,
        LAG(AvgQuarterCorrelation, 1) OVER (PARTITION BY PairID1, PairID2 ORDER BY Year, Quarter) AS PrevQuarterAvgCorrelation
    FROM 
        QuarterAverages
)
SELECT year,
       quarter,
       pairid1,
       pairid2,
       avgquartercorrelation,
       nvl(avgquartercorrelation - prevquarteravgcorrelation, 0) AS cchange
FROM QuarterChanges;

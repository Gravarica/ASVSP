#!/bin/bash
./../spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.10 ./batch/tradeInfo.py
#./../spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.10 ./batch/statisticMeasures.py
#./../spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.10 ./batch/dailyCandlesticks.py
#./../spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.10 ./batch/correlations.py
#./../spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.10 ./batch/minuteCandlesticks.py
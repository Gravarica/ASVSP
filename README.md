# Analiza trgovine kriptovalutama na sajtu Binance

- Projektni zadatak iz predmeta Arhitekture sistema velikih skupova podataka
- Cilj projekta je da se za skup podataka o trgovini kriptovaluta na Binance platformi, koji datira od 2017. godine do danas, pruže adekvatni odgovori na pitanja o dinamici tržišta kriptovaluta, kao i da se uz analizu podataka prikupljenih u realnom vremenu sa javnog
API-ja Binance platforme, pruži asistencija u povlačenju inteligentnih poteza prilikom trgovine

## Paketna obrada 
- Svrha paketne obrade istorijskih podataka je da se stekne uvid u prethodne trendove kretanja tržišta kriptovaluta sa ciljem otkrivanja učestalih obrazaca ponašanja koji bi se mogli koristiti u budućim inverstiranjima.
- Skup podataka je preuzet sa Kaggle platforme i sadrži istorijske podatke o jednominutnim svećnjacima (engl. Candlesticks) 1000 najpopularnijih parova razmena kriptovaluta, ukupne veličine oko 33 GB
- Sastoji se od 1000 _parquet_ datoteka gde svaka od njih predstavlja podatke o jednom paru kriptovaluta za koje se vrši razmena
- Skup podataka se može preuzeti sa sledećeg linka: [Podaci](https://www.kaggle.com/datasets/jorijnsmit/binance-full-history)

### Obrada podataka 
Na osnovu prethodno opisanog skupa podataka, paketna obrada teži da odgovori na sledeća pitanja:
1) Odrediti prosečan broj izvršeni razmena i obim trgovine u minuti za svaki dan. Zatim odrediti dan kada je bila najveći prosečan broj razmena u minuti kao i dane u 2022. godini kada je obim trgovine bio veći od prosečnog.
2) Za svaki dan u 2022. godini, odrediti period od sat vremena tokom kojeg je bila najniža ukupna vrednost bitkoina izražena u dolarima.
 

## Obrada u realnom vremenu 
- Obrada podataka u realnom vremenu je, pored istorijske obrade, drugi ključni faktor koji pomaže u donošenju odluka na tržištu kriptovaluta
- Binance pruža javno dostupni API kojem je moguće pristupiti pomoću veb soketa kako bi se pribavili podaci o tržištu u realnom vremenu
- Pristupom adresi "wss://stream.binance.com:9443/stream?streams=" uz navođenje toka podataka koji je potrebno osluškivati, moguće je prikupljati podatke o izabranim parovima kriptovaluta
- Primer osluškivanja para BTC-USDT bi izgledao ovako: "wss://stream.binance.com:9443/stream?streams=btcusdt@kline1m"

### Obrada podataka
Obrada u realnom vremenu teži da odgovori na sledeća pitanja:
1) Izračunati vrednost pokrentnog proseka cene bitkoina izražene u dolarima u intervalima od 10 sekundi 
2)
3)
 
## Dijagram Arhitekture rešenja
- Na narednoj slici, prikazan je dijagram arhitekture rešenja
  

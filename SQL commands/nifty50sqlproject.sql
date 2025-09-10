-- Create database
CREATE DATABASE IF NOT EXISTS Nifty50Index;
USE Nifty50Index;

-- 1. Sectors Table

CREATE TABLE IF NOT EXISTS Sectors (
    sector_id INT PRIMARY KEY,
    sector_name VARCHAR(100) NOT NULL
);

-- ================================
-- 2. Companies Table

CREATE TABLE IF NOT EXISTS Companies (
    company_id INT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    sector_id INT NOT NULL,
    FOREIGN KEY (sector_id)
        REFERENCES Sectors (sector_id)
);

-- ================================
-- 3. StockPrices Table

CREATE TABLE IF NOT EXISTS StockPrices (
    date DATE,
    symbol VARCHAR(20),
    open DECIMAL(20 , 10 ),
    high DECIMAL(20 , 10 ),
    low DECIMAL(20 , 10 ),
    close DECIMAL(20 , 10 ),
    volume BIGINT,
    PRIMARY KEY (date , symbol),
    FOREIGN KEY (symbol)
        REFERENCES Companies (symbol)
);


-- 3a. StockPrices Staging Table

CREATE TABLE IF NOT EXISTS StockPrices_Staging (
    date VARCHAR(20),
    symbol VARCHAR(20),
    open VARCHAR(20),
    high VARCHAR(20),
    low VARCHAR(20),
    close VARCHAR(20),
    volume VARCHAR(30)
);

-- Load StockPrices Data
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/nifty50_10yrs_up_to_2025.csv'
INTO TABLE StockPrices_Staging
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- Clean and Insert into StockPrices
INSERT INTO StockPrices (date, symbol, open, high, low, close, volume)
SELECT
    STR_TO_DATE(TRIM(date), '%Y-%m-%d'),
    TRIM(symbol),
    CAST(NULLIF(REPLACE(open, ',', ''), '') AS DECIMAL(16,4)),
    CAST(NULLIF(REPLACE(high, ',', ''), '') AS DECIMAL(16,4)),
    CAST(NULLIF(REPLACE(low, ',', ''), '') AS DECIMAL(16,4)),
    CAST(NULLIF(REPLACE(close, ',', ''), '') AS DECIMAL(16,4)),
    CAST(NULLIF(REPLACE(REPLACE(volume, '\r', ''), ',', ''), '') AS UNSIGNED)
FROM StockPrices_Staging
WHERE TRIM(date) <> '' AND TRIM(symbol) <> ''
ON DUPLICATE KEY UPDATE
    open = VALUES(open),
    high = VALUES(high),
    low = VALUES(low),
    close = VALUES(close),
    volume = VALUES(volume);





-- 4.NiftyIndex Staging

CREATE TABLE NiftyIndex_Staging (
    date_raw VARCHAR(50),
    price_raw VARCHAR(50),
    open_raw VARCHAR(50),
    high_raw VARCHAR(50),
    low_raw VARCHAR(50),
    vol_raw VARCHAR(50),
    change_pct_raw VARCHAR(50)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Nifty 50 Historical Data2015_2022.csv'
INTO TABLE NiftyIndex_Staging
FIELDS TERMINATED BY ','  
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(date_raw, price_raw, open_raw, high_raw, low_raw, vol_raw, change_pct_raw);

-- . NiftyIndex Table

CREATE TABLE NiftyIndex (
    id INT AUTO_INCREMENT PRIMARY KEY,
    `Date` DATE NOT NULL,
    `Price` DECIMAL(16 , 4 ),
    `Open` DECIMAL(16 , 4 ),
    `High` DECIMAL(16 , 4 ),
    `Low` DECIMAL(16 , 4 ),
    `Vol.` BIGINT,
    `Change %` DECIMAL(8 , 4 )
);

-- 3. Insert clean data from staging
INSERT INTO NiftyIndex (`Date`, `Price`, `Open`, `High`, `Low`, `Vol.`, `Change %`)
SELECT 
    CASE
      WHEN CAST(SUBSTRING_INDEX(TRIM(date_raw), '-', 1) AS UNSIGNED) > 31
        THEN STR_TO_DATE(TRIM(REPLACE(date_raw, UNHEX('C2A0'), '')), '%Y-%m-%d')
      ELSE STR_TO_DATE(TRIM(REPLACE(date_raw, UNHEX('C2A0'), '')), '%d-%m-%Y')
    END,

    CAST(REPLACE(REPLACE(price_raw, UNHEX('C2A0'), ''), ',', '') AS DECIMAL(16,4)),
    CAST(REPLACE(REPLACE(open_raw,  UNHEX('C2A0'), ''), ',', '') AS DECIMAL(16,4)),
    CAST(REPLACE(REPLACE(high_raw,  UNHEX('C2A0'), ''), ',', '') AS DECIMAL(16,4)),
    CAST(REPLACE(REPLACE(low_raw,   UNHEX('C2A0'), ''), ',', '') AS DECIMAL(16,4)),

    CASE
      WHEN RIGHT(TRIM(REPLACE(vol_raw, UNHEX('C2A0'), '')),1) = 'M'
        THEN FLOOR(CAST(REPLACE(REPLACE(REPLACE(vol_raw, 'M',''), ',', ''), UNHEX('C2A0'), '') AS DECIMAL(20,6)) * 1000000)
      WHEN RIGHT(TRIM(REPLACE(vol_raw, UNHEX('C2A0'), '')),1) = 'B'
        THEN FLOOR(CAST(REPLACE(REPLACE(REPLACE(vol_raw, 'B',''), ',', ''), UNHEX('C2A0'), '') AS DECIMAL(20,6)) * 1000000000)
      WHEN TRIM(REPLACE(vol_raw, UNHEX('C2A0'), '')) = '' THEN NULL
      ELSE CAST(REPLACE(REPLACE(vol_raw, UNHEX('C2A0'), ''), ',', '') AS UNSIGNED)
    END,

    CAST(REPLACE(REPLACE(change_pct_raw, UNHEX('C2A0'), ''), '%', '') AS DECIMAL(8,4))
FROM NiftyIndex_Staging;

-- Indexes

CREATE INDEX idx_companies_sector_id ON Companies(sector_id);
CREATE INDEX idx_stockprices_symbol ON StockPrices(symbol);
CREATE INDEX idx_stockprices_date ON StockPrices(date);
CREATE INDEX idx_niftyindex_date ON NiftyIndex(date);

-- ======================================================================================================================
-- Data Analysis
/*
********1.Sector Performance based on Monthly Average Close*************
*/
SELECT 
    s.sector_name,
    DATE_FORMAT(sp.date, '%Y-%m') AS month,
    ROUND(AVG(sp.close)) AS avg_close
FROM
    StockPrices sp
        JOIN
    Companies c ON sp.symbol = c.symbol
        JOIN
    Sectors s ON c.sector_id = s.sector_id
GROUP BY s.sector_name , `month`
ORDER BY `month` , s.sector_name;

/* =====================
**********2.Most Volatile Companies (Std. Dev. of Prices)**********

This query finds the companies with the most price fluctuations by calculating the standard deviation of their closing prices. Higher standard deviation means more volatility (bigger ups and downs).
*/

SELECT 
    symbol, ROUND(STDDEV(close)) AS volatality
FROM
    StockPrices
GROUP BY symbol
ORDER BY volatality DESC;

/*===================================
*******3.Top Gainers & Losers (Last 30 Days % Change)**********
This query looks at the last 30 days of data and calculates the percentage change in closing prices for each stock (from the lowest to highest price in that period). It's for gainers; for losers, change the ORDER BY to ASC.

*/

SELECT 
    symbol,
    ROUND(((MAX(close) - MIN(open)) / MIN(close)) * 100,
            2) AS percentage_change_month
FROM
    StockPrices
WHERE
    date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
GROUP BY symbol
ORDER BY percentage_change_month DESC;

/*==================================
**********4.NiftyIndex Trend vs Average Market Close of stocks **********
This query compares the daily Nifty Index price to the average closing price of all stocks on the same day, to see if the index matches the overall market.
*/
SELECT 
    ni.date,
    MAX(ni.price) AS nifty_index_price,
    ROUND(AVG(sp.close), 2) AS avg_market_close
FROM
    NiftyIndex AS ni
        JOIN
    StockPrices sp ON ni.date = sp.date
GROUP BY ni.date
ORDER BY ni.date;


/*================================
**********5.Highest Trading Volume Days**********
This query finds the top 10 days with the highest trading volume for any stock
*/

SELECT 
    symbol, `date`, volume
FROM
    StockPrices
ORDER BY volume DESC;



-- =================================================================================================================================
-- TECHNICAL ANALYSIS
/*
**********1.Simple Moving Average (SMA – 20-day example)**********
This query calculates a 20-day simple moving average (SMA) for each stock's closing price, which smooths out price data to identify trends.
*/

select symbol,`date`,`close`,round(avg(close) over(partition by symbol order by date rows 19 preceding), 2) as SMA_20
from StockPrices
order by symbol,SMA_20 desc;

/* =====================
**********2.Relative Strength Index (RSI – 14-day)**********
This query calculates the 14-day RSI, a momentum indicator that measures the speed and change of price movements to identify overbought or oversold conditions. It uses a CTE to first compute price changes, then average gains and losses.
*/
WITH profit_loss AS (
SELECT 
symbol,
`date`,
close - LAG(close) OVER (PARTITION BY symbol ORDER BY `date`) AS `change`
FROM StockPrices
),
RSI AS (
SELECT 
symbol,
`date`,
AVG(CASE WHEN `change` > 0 THEN `change` ELSE 0 END) OVER (PARTITION BY symbol ORDER BY `date` ROWS 13 PRECEDING) AS avg_profit,
AVG(CASE WHEN `change` < 0 THEN ABS(`change`) ELSE 0 END) OVER (PARTITION BY symbol ORDER BY `date` ROWS 13 PRECEDING) AS avg_loss
FROM profit_loss
)
SELECT 
symbol,
`date`,
100 - (100 / (1 + (avg_profit / NULLIF(avg_loss, 0)))) AS RSI_14
FROM RSI
ORDER BY  RSI_14 desc;

/*=========================================
**********3.Bollinger Bands**********
This query computes Bollinger Bands for each stock: a 20-day SMA (middle band) plus/minus 2 standard deviations (upper/lower bands) to measure volatility.
*/

select symbol,`date`,`close`,
round(avg(close) over (partition by symbol order by `date` rows 19 preceding), 2) as sma_20,
round(avg(close) over(partition by symbol order by `date` rows 19 preceding) + 2 * stddev(close) over (partition by symbol order by `date` rows 19 preceding),2) as upper_bandwidth,
round(avg(close) over(partition by symbol order by `date` rows 19 preceding) - 2 * stddev(close) over (partition by symbol order by `date` rows 19 preceding),2) as lower_bandwidth
from StockPrices
order by upper_bandwidth,`date`;

/*==================================================
********4.VWAP (Volume Weighted Average Price)********

VWAP gives the average price weighted by volume over a period ,commonly a day for intraday, or over multiple days if needed.

*/

select symbol,`date`,round(sum((high+low+`close`)/3 * volume)/sum(volume),2) as VWAP
from StockPrices
group by symbol,`date`
order by `date`,VWAP;

-- Price Action============================================================================================================================
/*
**********1.Daily Candle Type (Bullish / Bearish)**********
*/

select symbol,`date`,high,low,`open`,`close`,volume,
case
when `close`>`open` then 'bullish Candle Stick'
when `open` > `close` then 'bearish Candle Stick'
else 'Doji Stick'
end as candle_type
from StockPrices
order by volume;

/*===================
**********2.(Swing Highs / Swing Lows) Identify Swing Points**********
*/

select symbol,`date`,high,low,
case
when high > lag(high,1) over(partition by symbol order by `date`)
and high > lead(high, 1) over(partition by symbol order by `date`)
then 'Swing High'
when low < lag(low,1) over(partition by symbol order by `date`)
and low< lead(low,1) over(partition by symbol order by `date`)
then 'Swing Low'
End as Swing_Points
from StockPrices
order by symbol,date;

/*=====================================================================
**********3.Support & Resistance**********
*/

with swing_points as(
select symbol,`date`,high,low,
case
when high>lag(high,1) over(partition by symbol order by `date`)
and high>lead(high,1) over(partition by symbol order by `date`)
then high
end as swing_high,
case
when low<lag(low,1) over(partition by symbol order by `date`)
and low<lead(low,1) over(partition by symbol order by `date`)
then low
end as swing_low
from StockPrices
),
Zone as(
select symbol,
round(swing_high,0) as resistance,
round(swing_low,0) as support,
count(*) as touch_count
from swing_points
where swing_high is not null or swing_low is not null
group by symbol,resistance,support
having count(*) >=10 
)
select * from zone
order by symbol,resistance,support;

/*===================================
********* 4.Breakouts & breakdowns*********
*/
with swing_points as(
select symbol,`date`,high,low,
case
when high>lag(high,1) over(partition by symbol order by `date`)
and high>lead(high,1) over(partition by symbol order by `date`)
then high
end as swing_high,
case
when low<lag(low,1) over(partition by symbol order by `date`)
and low<lead(low,1) over(partition by symbol order by `date`)
then low
end as swing_low
from StockPrices
),
Zone as(
select symbol,
round(swing_high,0) as resistance,
round(swing_low,0) as support,
count(*) as touch_count
from swing_points
where swing_high is not null or swing_low is not null
group by symbol,round(swing_high,0),round(swing_low,0)
having count(*) >=10 
)
select sp.symbol,sp.`date`,sp.`close`,z.resistance,z.support,
case
when sp.`close` > z.resistance then 'Breakout above Resistance'
when sp.`close` < z.support then 'Breakout below Support'
end as breakout
from StockPrices sp
join zone z
on sp.symbol = z.symbol
and (
(z.resistance IS NOT NULL AND sp.`close` > z.resistance AND sp.`close` <= z.resistance * 1.05)
or (z.support IS NOT NULL AND sp.`close` < z.support AND sp.`close` >= z.support * 0.95)
 )
ORDER BY sp.symbol, sp.`date`;
;
-- ==============================================================================================


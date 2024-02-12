-- Databricks notebook source
CREATE OR REPLACE TEMPORARY VIEW ticker_value_update AS
SELECT DISTINCT
  tv.Ticker,CAST(tv.variable as DATE) AS stock_date, Open, Close, High, Low, Volume
FROM 
  bronze.ticker_value tv
PIVOT (
  MAX(CAST(tv.value AS DECIMAL(20,2)))
  FOR Value_type IN ('1. open' AS Open, '4. close' AS Close, '2. high' as High, '3. low' as Low, '5. volume' as Volume)
)
LEFT JOIN (SELECT MIN(CAST(variable as DATE)) AS min_variable, Ticker
  FROM
    bronze.ticker_value
  WHERE 
    value IS NOT NULL
  GROUP BY Ticker
) tv1
  ON tv.Ticker = tv1.Ticker 
WHERE CAST(variable AS Date) >= CAST(tv1.min_variable AS DATE)

-- COMMAND ----------

MERGE INTO silver.s_ticker_value
USING ticker_value_update
ON silver.s_ticker_value.Ticker=ticker_value_update.Ticker
and silver.s_ticker_value.stock_date = ticker_value_update.stock_date
WHEN MATCHED THEN
UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #sp_silver_ticker_value = spark.sql('''
-- MAGIC #SELECT DISTINCT
-- MAGIC #  tv.Ticker,CAST(tv.variable as DATE) AS stock_date, Open, Close, High, Low, Volume
-- MAGIC #FROM 
-- MAGIC #  bronze.ticker_value tv
-- MAGIC #PIVOT (
-- MAGIC #  MAX(CAST(tv.value AS DECIMAL(20,2)))
-- MAGIC #  FOR Value_type IN ('1. open' AS Open, '4. close' AS Close, '2. high' as High, '3. low' as Low, '5. volume' as #Volume)
-- MAGIC #)
-- MAGIC #LEFT JOIN (SELECT MIN(CAST(variable as DATE)) AS min_variable, Ticker
-- MAGIC #  FROM
-- MAGIC #    bronze.ticker_value
-- MAGIC #  WHERE 
-- MAGIC #    value IS NOT NULL
-- MAGIC #  GROUP BY Ticker
-- MAGIC #) tv1
-- MAGIC #  ON tv.Ticker = tv1.Ticker 
-- MAGIC #WHERE CAST(variable AS Date) >= CAST(tv1.min_variable AS DATE)
-- MAGIC #'''
-- MAGIC #)
-- MAGIC #sp_silver_ticker_value.distinct().write.mode("Overwrite").option("OverwriteSchema", "true").format("delta").save("/#FileStore/silver/s_ticker_value") 
-- MAGIC #spark.sql("CREATE TABLE IF NOT EXISTS silver.s_ticker_value USING DELTA LOCATION '/FileStore/silver/s_ticker_value'")

-- COMMAND ----------



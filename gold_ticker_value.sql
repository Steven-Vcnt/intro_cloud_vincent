-- Databricks notebook source
CREATE OR REPLACE TEMPORARY VIEW gold_ticker_value_update AS
SELECT DISTINCT
  tv.Ticker, tm.name, tv.stock_date, tv.Open, tv.Close, tv.High, tv.Low, tv.Volume, (tv.Close - tv.Open)/tv.Open * 100 AS intra_day_evolution
  ,AVG(tv.Close) 
  OVER (
       ORDER BY tm.name, tv.stock_date
       ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
       ) AS 1D_Moving_Average
  , (tv.Close - 1D_Moving_Average)/1D_Moving_Average * 100 AS day_evolution
  ,AVG(tv.Close) 
    OVER (
        ORDER BY tm.name, tv.stock_date
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS 5D_Moving_Average
  , (tv.Close - 5D_Moving_Average)/5D_Moving_Average * 100 AS week_evolution
  ,AVG(tv.Close) 
    OVER (
        ORDER BY tm.name, tv.stock_date
        ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
        ) AS 1M_Moving_Average
        , (tv.Close - 1M_Moving_Average)/1M_Moving_Average * 100 AS month_evolution
  ,AVG(tv.Close) 
    OVER (
        ORDER BY tm.name, tv.stock_date
        ROWS BETWEEN 180 PRECEDING AND CURRENT ROW
        ) AS 6M_Moving_Average
        , (tv.Close - 6M_Moving_Average)/6M_Moving_Average * 100 AS semester_evolution
    ,AVG(tv.Close)
    OVER (
        ORDER BY tm.name, tv.stock_date
        ROWS BETWEEN 360 PRECEDING AND CURRENT ROW
        ) AS 1Y_Moving_Average
    , (tv.Close - 1Y_Moving_Average)/1Y_Moving_Average * 100 AS year_evolution

FROM 
  silver.s_ticker_value tv
LEFT JOIN silver.s_ticker_metadata tm
  ON tm.Ticker = tv.Ticker
--WHERE tv.Ticker = 'GOOGL'

-- COMMAND ----------

MERGE INTO gold.g_ticker_value
USING gold_ticker_value_update
ON gold.g_ticker_value.Ticker=gold_ticker_value_update.Ticker
and gold.g_ticker_value.stock_date = gold_ticker_value_update.stock_date
WHEN MATCHED THEN
UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #sp_gold_ticker_value = spark.sql('''
-- MAGIC #SELECT 
-- MAGIC #  tv.Ticker, tm.name, tv.stock_date, tv.Open, tv.Close, tv.High, tv.Low, tv.Volume, (tv.Close - tv.Open)/tv.Open * #100 AS intra_day_evolution
-- MAGIC #  ,AVG(tv.Close) 
-- MAGIC #  OVER (
-- MAGIC #       ORDER BY tm.name, tv.stock_date
-- MAGIC #       ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
-- MAGIC #       ) AS 1D_Moving_Average
-- MAGIC #  , (tv.Close - 1D_Moving_Average)/1D_Moving_Average * 100 AS day_evolution
-- MAGIC #  ,AVG(tv.Close) 
-- MAGIC #    OVER (
-- MAGIC #        ORDER BY tm.name, tv.stock_date
-- MAGIC #        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
-- MAGIC #        ) AS 5D_Moving_Average
-- MAGIC #  , (tv.Close - 5D_Moving_Average)/5D_Moving_Average * 100 AS week_evolution
-- MAGIC #  ,AVG(tv.Close) 
-- MAGIC #    OVER (
-- MAGIC #        ORDER BY tm.name, tv.stock_date
-- MAGIC #        ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
-- MAGIC #        ) AS 1M_Moving_Average
-- MAGIC #        , (tv.Close - 1M_Moving_Average)/1M_Moving_Average * 100 AS month_evolution
-- MAGIC #  ,AVG(tv.Close) 
-- MAGIC #    OVER (
-- MAGIC #        ORDER BY tm.name, tv.stock_date
-- MAGIC #        ROWS BETWEEN 180 PRECEDING AND CURRENT ROW
-- MAGIC #        ) AS 6M_Moving_Average
-- MAGIC #        , (tv.Close - 6M_Moving_Average)/6M_Moving_Average * 100 AS semester_evolution
-- MAGIC #    ,AVG(tv.Close)
-- MAGIC #    OVER (
-- MAGIC #        ORDER BY tm.name, tv.stock_date
-- MAGIC #        ROWS BETWEEN 360 PRECEDING AND CURRENT ROW
-- MAGIC #        ) AS 1Y_Moving_Average
-- MAGIC #    , (tv.Close - 1Y_Moving_Average)/1Y_Moving_Average * 100 AS year_evolution
-- MAGIC #FROM 
-- MAGIC #  silver.s_ticker_value tv
-- MAGIC #LEFT JOIN silver.s_ticker_metadata tm
-- MAGIC #  ON tm.Ticker = tv.Ticker
-- MAGIC #'''
-- MAGIC #)
-- MAGIC #sp_gold_ticker_value.distinct().write.mode("Overwrite").option("OverwriteSchema", "true").format("delta").save("/#FileStore/gold/g_ticker_value") 
-- MAGIC #spark.sql("CREATE TABLE IF NOT EXISTS gold.g_ticker_value USING DELTA LOCATION '/FileStore/gold/g_ticker_value'")

-- COMMAND ----------



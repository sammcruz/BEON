-- Databricks notebook source
-- MAGIC %md
-- MAGIC You have been assigned by Newark International Airport's Engineering Manager to conduct an\
-- MAGIC analysis of the airport's current capabilities and identify key metrics that can improve the\
-- MAGIC airport's administration. You have been provided with several data files: nyc_airlines.csv,\
-- MAGIC nyc_airports.csv, nyc_flights.csv, nyc_planes.csv, and nyc_weather.csv. Your task is to develop\
-- MAGIC a Dashboard using a Notebook and/or BI tool to present the following information:
-- MAGIC

-- COMMAND ----------

-- MAGIC %py
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC # Step 1: Read the CSV file using Pandas and create a DataFrame
-- MAGIC file_path = "/Workspace/Users/samantha@poli.ufrj.br/beon/nyc_airlines.csv"
-- MAGIC pandas_df = pd.read_csv(file_path)
-- MAGIC
-- MAGIC # Step 2: Convert the Pandas DataFrame to PySpark DataFrame
-- MAGIC spark_df = spark.createDataFrame(pandas_df)
-- MAGIC
-- MAGIC # Step 3: Register the PySpark DataFrame as a temporary table
-- MAGIC table_name = "nyc_airlines" 
-- MAGIC spark_df.createOrReplaceTempView(table_name)
-- MAGIC
-- MAGIC #other tables
-- MAGIC
-- MAGIC pandas_df = pd.read_csv("/Workspace/Users/samantha@poli.ufrj.br/beon/nyc_flights_fixed.csv")
-- MAGIC spark_df = spark.createDataFrame(pandas_df)
-- MAGIC spark_df.createOrReplaceTempView("nyc_flights_fixed")
-- MAGIC
-- MAGIC pandas_df = pd.read_csv("/Workspace/Users/samantha@poli.ufrj.br/beon/nyc_planes.csv")
-- MAGIC spark_df = spark.createDataFrame(pandas_df)
-- MAGIC spark_df.createOrReplaceTempView("nyc_planes")
-- MAGIC pandas_df = pd.read_csv("/Workspace/Users/samantha@poli.ufrj.br/beon/nyc_airports.csv")
-- MAGIC spark_df = spark.createDataFrame(pandas_df)
-- MAGIC spark_df.createOrReplaceTempView("nyc_airports")
-- MAGIC
-- MAGIC pandas_df = pd.read_csv("/Workspace/Users/samantha@poli.ufrj.br/beon/nyc_weather.csv")
-- MAGIC spark_df = spark.createDataFrame(pandas_df)
-- MAGIC spark_df.createOrReplaceTempView("nyc_weather")

-- COMMAND ----------

-- tables:
-- nyc_airlines
-- nyc_flights_fixed
-- nyc_planes
-- nyc_airports
-- nyc_weather

-- COMMAND ----------

-- 1. Determine the number of distinct destinations connected to the airport.
select distinct dest
from nyc_flights_fixed
where origin = 'EWR'

-- COMMAND ----------

-- Create a ranking of airlines based on the number of flights they operate.
with fligths_per_airline as (
  select count(n.flight) as total_flights, a.name
  from  nyc_flights_fixed n
  join nyc_airlines a
    on n.carrier = a.carrier
  group by a.name
)
select name as airline, row_number() over(order by total_flights desc) as rank_airline, total_flights
from fligths_per_airline

-- COMMAND ----------

-- avg delay by day
select concat(day, '/', month, '/', year) as date, round(avg(dep_delay),3) as avg_delay
from  nyc_flights_fixed 
group by 1

-- COMMAND ----------

-- avg delay by day without negative delay number
select concat(day, '/', month, '/', year) as date, round(avg(dep_delay),3) as avg_delay
from  nyc_flights_fixed 
where dep_delay > 0
group by 1

-- COMMAND ----------

-- monthly delay sum
  select concat(month, '-', year) as date, 
  sum(dep_delay) as sum_delay
from  nyc_flights_fixed 
where dep_delay > 0
group by 1

-- COMMAND ----------

with month_delay as (
  select concat(year, '-', 
  case when month <= 9 then concat(0,month) else month end) as date, 
  sum(dep_delay) month_delay
from  nyc_flights_fixed 
where dep_delay > 0
group by 1
)

select date, 
  month_delay,
  sum(month_delay) over (order by date asc) as cumsum
from  month_delay
order by date

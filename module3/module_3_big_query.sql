
-- Q1 with query, other than seeing DETAILS tab
select count(*) from `ny_taxi.green_taxi_data_2022`;


-- Q2
select count(distinct PULocationID) from `ny_taxi.green_taxi_data_2022`;


-- Q3
select count(*)
from `ny_taxi.green_taxi_data_2022`
where fare_amount = 0;


-- Q4
create or replace table `ny_taxi.green_taxi_data_2022_partitioned_clustered`
partition by
  date(lpep_pickup_datetime)
cluster by
  PULocationID AS
select * from `ny_taxi.green_taxi_data_2022`;


-- Q5
-- Scan 12.82 MB
-- Materialized non-partitioned table
select distinct PULocationID from `ny_taxi.green_taxi_data_2022`
where date(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30';

-- Scan 0 B
-- Materialized partitioned clustered table
select distinct PULocationID from `ny_taxi.green_taxi_data_2022_partitioned_clustered`
where date(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30';



--Bonus question
-- Scan 0 B
SELECT count(*) FROM `ny_taxi.green_taxi_data_2022`;

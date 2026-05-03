-- valor medio por viagem por mês
  SELECT
  year,
  month,
  COUNT(*) AS total_trips,
  ROUND(AVG(total_amount), 2) AS avg_total_amount,
  (ROUND(AVG(total_amount), 2)  /  NULLIF(COUNT(*), 0) ) as avg_total_amount_per_trip
FROM `gold-nyc-taxi-case`.fact_trips
GROUP BY
  year,
  month
ORDER BY
  year,
  month;


-- receita por km
SELECT
  year,
  month,
  SUM(total_amount) as revenue,
  SUM(trip_distance) as km,
  ROUND(SUM(total_amount) / NULLIF(SUM(trip_distance), 0), 2) AS revenue_per_km
FROM `gold-nyc-taxi-case`.fact_trips
GROUP BY year, month
ORDER BY year, month;

-- tempo médio de duração das corridas
SELECT
  HOUR(tpep_pickup_datetime) AS hour,
  ROUND(AVG(
    (UNIX_TIMESTAMP(tpep_dropoff_datetime) - UNIX_TIMESTAMP(tpep_pickup_datetime)) / 60
  ), 2) AS avg_trip_duration_minutes
FROM `gold-nyc-taxi-case`.fact_trips
GROUP BY HOUR(tpep_pickup_datetime)
ORDER BY hour;


-- melhor horário de gorjeta
SELECT
  HOUR(tpep_pickup_datetime) AS hour,
  ROUND(AVG(tip_amount), 2) AS avg_tip_amount
FROM `gold-nyc-taxi-case`.fact_trips
GROUP BY HOUR(tpep_pickup_datetime)
ORDER BY avg_tip_amount DESC;

-- valor médio da tarifa por mês
SELECT
  year,
  month,
  ROUND(AVG(fare_amount), 2) AS avg_fare_amount
FROM `gold-nyc-taxi-case`.fact_trips
GROUP BY year, month
ORDER BY year, month;

-- qtd passageiros por horário
SELECT
  HOUR(tpep_pickup_datetime) AS hour,
  SUM(passenger_count) AS total_passengers
FROM `gold-nyc-taxi-case`.fact_trips
GROUP BY HOUR(tpep_pickup_datetime)
ORDER BY total_passengers DESC;

-- top 5 horários com mais passageiros (média) por corrida
SELECT
  HOUR(tpep_pickup_datetime) AS hour,
  COUNT(*) AS total_trips,
  SUM(passenger_count) AS total_passengers,
  ROUND(AVG(passenger_count), 2) AS avg_passengers_per_trip
FROM `gold-nyc-taxi-case`.fact_trips
GROUP BY HOUR(tpep_pickup_datetime)
ORDER BY avg_passengers_per_trip DESC
LIMIT 5;

-- qtd de corridas com valor, porém sem passageiros
SELECT
  COUNT(*) AS total_trips,
  SUM(CASE WHEN passenger_count IS NULL OR passenger_count = 0 THEN 1 ELSE 0 END) AS invalid_passenger_trips
FROM `gold-nyc-taxi-case`.fact_trips
WHERE total_amount > 0;

-- check na qualidade dos dados
SELECT
  CASE
    WHEN passenger_count IS NULL THEN 'NULL'
    WHEN passenger_count = 0 THEN 'ZERO'
    ELSE 'VALIDOS'
  END AS passenger_status,
  COUNT(*) AS total_trips
FROM `gold-nyc-taxi-case`.fact_trips
GROUP BY 1;
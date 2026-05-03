/* Qual a média de passageiros (passenger\_count) por cada hora do dia
que pegaram táxi no mês de maio considerando todos os táxis da
frota?
 */

 -- versão usando modelo dimensional e colunas de ano e mês particionados para performance
SELECT
  hour(tpep_pickup_datetime) AS hour,
  ROUND(AVG(passenger_count), 2) AS avg_passenger_count
FROM workspace.`gold-nyc-taxi-case`.fact_trips
WHERE year = 2023
  AND month = 5
GROUP BY 1
ORDER BY 1;

-- versão usando fato flat que já tem coluna de hora
SELECT
  hour,
  ROUND(AVG(passenger_count), 2) AS avg_passenger_count
FROM workspace.`gold-nyc-taxi-case`.fact_trips_flat
WHERE year = 2023
  AND month = 5
GROUP BY hour
ORDER BY hour;



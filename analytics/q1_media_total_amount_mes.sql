/* Qual a média de valor total (total_amount) recebido em um mês
considerando todos os yellow táxis da frota?

Resposta: Não encontrei no dataset dados sobre a data de pagemento, portanto assumi como data do início da corrida para responder a questão. 
Se fosse no mundo real eu iria me certificar sobre qual data usar. Também pensei em usar a data que vem no nome do arquivo, mas achei
meio over para um desafio. Se quiserem posso levar ano e mês do nome do arquivo para uma coluna e fazer a consulta por ele.*/

-- versão usando funções sobre campos de data, sem usar as colunas de particionamento

SELECT
  YEAR(tpep_pickup_datetime) AS year,
  MONTH(tpep_pickup_datetime) AS month,
  ROUND(AVG(total_amount),2) AS avg_total_amount
FROM `gold-nyc-taxi-case`.fact_trips
GROUP BY
  YEAR(tpep_pickup_datetime),
  MONTH(tpep_pickup_datetime)
ORDER BY
  year,
  month;

  -- versão usando as colunas já prontas de ano e mês, evitando usar funções SQL
  SELECT
  year,
  month,
   ROUND(AVG(total_amount), 2) AS avg_total_amount
FROM `gold-nyc-taxi-case`.fact_trips
GROUP BY
  year,
  month
ORDER BY
  year,
  month;
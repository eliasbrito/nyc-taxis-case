"""
    Este código gera a camada ouro a partir da prata, mas tem uns detalhes:
    1 - Sou partidário do uso de modelagem dimensional na camada ouro, por isso escolhi essa abordagem. 
        Mas sou aberto a fazer o que realmente soluciona o problema do cliente. A escolha do Star schema aqui
        se baseia no fato de que algumas ferramentas de DataViz, como Power BI, são mais performáticas com esse tipo de modelo.
    2 - Acrescentei também uma tabela única com todas as dimensões degeneradas. Essa abordagem pode facilitar o uso para alguns
        analistas/cientistas de dados e reduz a complexidade de queries evitando joins.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, hour,
    sha2, concat_ws
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


# =========================================================
# SPARK
# =========================================================
def create_spark_session():
    return (
        SparkSession.builder
        .appName("silver-to-gold")
        .getOrCreate()
    )


# =========================================================
# READ SILVER
# =========================================================
def read_silver(spark):
    path = "s3://silver-nyc-taxis-case/yellow_taxi/"
    return spark.read.format("delta").load(path)


# =========================================================
# DIM LOCATION
# =========================================================
def build_dim_location(df):

    base = df.select(
        col("pulocationid").alias("location_id")
    ).union(
        df.select(col("dolocationid").alias("location_id"))
    ).distinct()

    dim_location = base.withColumn(
        "location_sk",
        sha2(col("location_id").cast("string"), 256)
    )

    return dim_location


# =========================================================
# DIM DATETIME
# =========================================================
def build_dim_datetime(df):

    base = df.select(col("tpep_pickup_datetime").alias("datetime")).union(
        df.select(col("tpep_dropoff_datetime").alias("datetime"))
    ).distinct()

    dim_datetime = base.select(
        col("datetime"),
        year("datetime").alias("year"),
        month("datetime").alias("month"),
        dayofmonth("datetime").alias("day"),
        hour("datetime").alias("hour")
    ).withColumn(
        "datetime_sk",
        sha2(col("datetime").cast("string"), 256)
    )

    return dim_datetime


# =========================================================
# DIM PAYMENT
# =========================================================
def build_dim_payment(df):

    base = df.select("payment_type").distinct()

    dim_payment = base.withColumn(
        "payment_sk",
        sha2(col("payment_type").cast("string"), 256)
    )

    return dim_payment


# =========================================================
# FACT TABLE
# =========================================================
def build_fact_trips(df, dim_location, dim_datetime, dim_payment):

    # preparar dimensões
    dim_pickup = dim_location.withColumnRenamed("location_id", "pulocationid")
    dim_dropoff = dim_location.withColumnRenamed("location_id", "dolocationid")

    dim_dt = dim_datetime.withColumnRenamed("datetime", "tpep_pickup_datetime")

    # join pickup location
    fact = df.join(dim_pickup, "pulocationid", "left") \
             .withColumnRenamed("location_sk", "pickup_location_sk")

    # join dropoff location
    fact = fact.join(dim_dropoff, "dolocationid", "left") \
               .withColumnRenamed("location_sk", "dropoff_location_sk")

    # join datetime
    fact = fact.join(dim_dt, "tpep_pickup_datetime", "left") \
               .withColumnRenamed("datetime_sk", "pickup_datetime_sk")

    # join payment
    fact = fact.join(dim_payment, "payment_type", "left")

    fact_final = fact.select(
        col("vendorid").alias("vendor_id"),

        col("pickup_datetime_sk"),
        col("pickup_location_sk"),
        col("dropoff_location_sk"),
        col("payment_sk"),

        col("tpep_pickup_datetime"),
        col("tpep_dropoff_datetime"),

        col("passenger_count"),
        col("trip_distance"),
        col("fare_amount"),
        col("tip_amount"),
        col("tolls_amount"),
        col("total_amount"),

        year("tpep_pickup_datetime").alias("year"),
        month("tpep_pickup_datetime").alias("month")
    )

    return fact_final


# =========================================================
# FACT FLAT
# =========================================================
def build_fact_trips_flat(df):

    return df.select(
        col("vendorid").alias("vendor_id"),

        col("tpep_pickup_datetime"),
        col("tpep_dropoff_datetime"),

        col("pulocationid").alias("pickup_location_id"),
        col("dolocationid").alias("dropoff_location_id"),

        col("payment_type"),

        col("passenger_count"),
        col("trip_distance"),
        col("fare_amount"),
        col("tip_amount"),
        col("tolls_amount"),
        col("total_amount"),

        year("tpep_pickup_datetime").alias("year"),
        month("tpep_pickup_datetime").alias("month"),
        dayofmonth("tpep_pickup_datetime").alias("day"),
        hour("tpep_pickup_datetime").alias("hour")
    )


# =========================================================
# WRITE DELTA
# =========================================================
def write_delta(df, path, table_name, spark, partition_cols=None):

    writer = df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true")

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.save(path)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING DELTA
        LOCATION '{path}'
    """)


# =========================================================
# GOLD LAYER
# =========================================================
def write_gold(fact_df, fact_flat_df, dim_location, dim_datetime, dim_payment, spark):

    write_delta(
        fact_df,
        "s3://gold-nyc-taxis-case/fact_trips/",
        "workspace.`gold-nyc-taxi-case`.fact_trips",
        spark,
        partition_cols=["year", "month"]
    )

    write_delta(
        dim_location,
        "s3://gold-nyc-taxis-case/dim_location/",
        "workspace.`gold-nyc-taxi-case`.dim_location",
        spark
    )

    write_delta(
        dim_datetime,
        "s3://gold-nyc-taxis-case/dim_datetime/",
        "workspace.`gold-nyc-taxi-case`.dim_datetime",
        spark
    )

    write_delta(
        dim_payment,
        "s3://gold-nyc-taxis-case/dim_payment/",
        "workspace.`gold-nyc-taxi-case`.dim_payment",
        spark
    )

    write_delta(
        fact_flat_df,
        "s3://gold-nyc-taxis-case/fact_trips_flat/",
        "workspace.`gold-nyc-taxi-case`.fact_trips_flat",
        spark,
        partition_cols=["year", "month"]
    )


# =========================================================
# OPTIMIZATION
# =========================================================
def optimize_tables(spark):

    try:
        spark.sql("""
            OPTIMIZE workspace.`gold-nyc-taxi-case`.fact_trips
            ZORDER BY (pickup_location_sk, dropoff_location_sk, payment_sk)
        """)
    except Exception as e:
        print(f"OPTIMIZE não suportado: {e}")


# =========================================================
# PIPELINE
# =========================================================
def main():

    spark = create_spark_session()

    print("Lendo dados da Silver...")
    df = read_silver(spark)

    print("Criando dimensões...")
    dim_location = build_dim_location(df)
    dim_datetime = build_dim_datetime(df)
    dim_payment = build_dim_payment(df)

    print("Construindo fato...")
    fact_df = build_fact_trips(df, dim_location, dim_datetime, dim_payment)

    print("Construindo fato flat...")
    fact_flat_df = build_fact_trips_flat(df)

    print("Escrevendo Gold...")
    write_gold(fact_df, fact_flat_df, dim_location, dim_datetime, dim_payment, spark)

    print("Otimizando...")
    optimize_tables(spark)

    print("Pipeline Gold finalizado com sucesso!")


if __name__ == "__main__":
    main()
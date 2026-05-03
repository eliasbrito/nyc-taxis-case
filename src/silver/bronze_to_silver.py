"""
    Esse código tem por objetivo levar os dados do bucket de Bronze para o bucket de Silver.
    Problemas foram encontrados na tipagem de dados ao tentar carregar diferentes arquivos em um mesmo dataset.
    Já havia lidado com esse tipo de problema em datasets Pandas, mas com Spark foi a primeira vez.
    Depois de testar algumas abordagens, até mesmo sugeridas pelo chatgpt, decidi ler um arquivo por vez,
    tratando cada arquivo e o adequando a estrutura de dados que eu precisava.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, lit

def create_spark_session():
    return (
        SparkSession.builder
        .appName("bronze-to-silver-yellow-taxi")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

def get_list_of_files(path):
    """
    Lista todos os arquivos parquet no diretório de forma recursiva usando dbutils.
    """
    files = []
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(SparkSession.builder.getOrCreate())
    
    def recursive_list(p):
        for f in dbutils.fs.ls(p):
            if f.isDir():
                recursive_list(f.path)
            elif f.path.endswith(".parquet"):
                files.append(f.path)
    
    recursive_list(path)
    return files

def process_file(spark, file_path):
    """
    Lê um único arquivo e força a padronização de tipos e nomes.
    """
    try:
        temp_df = spark.read.parquet(file_path)
        
        # Normaliza nomes para minúsculo
        temp_df = temp_df.toDF(*[c.lower() for c in temp_df.columns])
        
        # Dicionário de tipos para garantir consistência no Union
        columns_to_fix = {
            "vendorid": "long", "pulocationid": "long", "dolocationid": "long", "payment_type": "long",
            "tpep_pickup_datetime": "timestamp", "tpep_dropoff_datetime": "timestamp",
            "passenger_count": "double", "trip_distance": "double", "ratecodeid": "double",
            "fare_amount": "double", "extra": "double", "mta_tax": "double", "tip_amount": "double",
            "tolls_amount": "double", "improvement_surcharge": "double", "total_amount": "double",
            "congestion_surcharge": "double", "airport_fee": "double"
        }
        
        selected_cols = []
        for name, dtype in columns_to_fix.items():
            if name in temp_df.columns:
                selected_cols.append(col(name).cast(dtype))
            else:
                # Se a coluna não existir no arquivo (comum em arquivos antigos), cria como NULL
                selected_cols.append(lit(None).cast(dtype).alias(name))
        
        return temp_df.select(selected_cols)
    except Exception as e:
        print(f"Erro ao processar arquivo {file_path}: {e}")
        return None

def main():
    spark = create_spark_session()
    base_path = "s3://bronze-nyc-taxis-case/yellow_taxi/"
    output_path = "s3://silver-nyc-taxis-case/yellow_taxi/"
    table_name = "`silver-nyc-taxi-case`.yellow_taxi"
    
    print("Listando arquivos na Bronze...")
    all_files = get_list_of_files(base_path)
    print(f"Total de arquivos encontrados: {len(all_files)}")
    
    final_df = None
    
    for file in all_files:
        current_df = process_file(spark, file)
        
        if current_df:
            if final_df is None:
                final_df = current_df
            else:
                final_df = final_df.unionByName(current_df)

    if final_df:
        print("Aplicando enriquecimento e filtros...")
        final_df = (
            final_df.dropDuplicates()
            .filter(col("tpep_pickup_datetime").isNotNull())
            .withColumn("year", year(col("tpep_pickup_datetime")))
            .withColumn("month", month(col("tpep_pickup_datetime")))
            .withColumn("day", dayofmonth(col("tpep_pickup_datetime")))
        )
      
        print(f"Gravando dados em formato Delta em {output_path}...")
        (
            final_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("year", "month", "day")
            .save(output_path)
        )
        
        print(f"Sincronizando tabela {table_name} no catálogo...")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{output_path}'")
        
        print("=== PROCESSO CONCLUÍDO COM SUCESSO! ===")
    else:
        print("ERRO: Nenhum dado foi processado.")

if __name__ == "__main__":
    main()
import boto3
import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data" # peguei essa url ao inspecionar o elemento. Isso facilita a vida para automatizar o processo de download

YEAR = "2023"
MONTHS = ["01", "02", "03", "04", "05"] # apenas meses selecionados. Em ambiente produtivo teríamos uma lógica de carga temporal que contempla reprocessamento

BUCKET = "bronze-nyc-taxis-case"
PREFIX = "yellow_taxi"

# pega credenciais do Databricks Secrets
access_key = dbutils.secrets.get(scope="aws-secrets", key="access_key")
secret_key = dbutils.secrets.get(scope="aws-secrets", key="secret_key")

# cria cliente S3 com credencial
s3 = boto3.client(
    "s3",
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name="us-east-2"  # ajuste se necessário
)

def download_and_upload(month):
    file_name = f"yellow_tripdata_{YEAR}-{month}.parquet"
    url = f"{BASE_URL}/{file_name}"

    s3_key = f"{PREFIX}/{YEAR}/{month}/{file_name}"

    print(f"\n[DOWNLOAD] {url}")
    print(f"[UPLOAD] s3://{BUCKET}/{s3_key}")

    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()

    s3.upload_fileobj(response.raw, BUCKET, s3_key)

    print(f"[OK] {file_name} enviado com sucesso")


def main():
    print("===== INÍCIO DA INGESTÃO BRONZE =====")

    for month in MONTHS:
        download_and_upload(month)

    print("\n===== INGESTÃO FINALIZADA =====")

main()
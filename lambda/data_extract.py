import json
import sys
import requests
import base64
import pandas as pd
from datetime import datetime
import boto3
import io

def lambda_handler(event, context):
    # Monta o payload e codifica em Base64
    payload = {
        "language": "pt-br",
        "pageNumber": 1,
        "pageSize": 200,
        "index": "IBOV",
        "segment": "1"
    }
    payload_b64 = base64.b64encode(json.dumps(payload).encode("utf-8")).decode()

    # Faz a requisição
    url = f"https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/{payload_b64}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Converte em DataFrame
    df = pd.DataFrame(data["results"])

    # Remove os pontos
    df['theoricalQty'] = df['theoricalQty'].str.replace('.', '', regex=False)

    # Data de extração 
    pregao_date = datetime.today().strftime('%Y-%m-%d')

    df['pregao_date'] = pregao_date

    nome_arquivo = f"raw/{pregao_date}/bovespa_{pregao_date}.parquet"

    bucket_s3 = "bucket-bovespa-653795152707"

    # Converte para Parquet em memória
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    # Envia para o S3
    s3 = boto3.client('s3')

    s3.put_object(Bucket=bucket_s3, Key=nome_arquivo, Body=buffer.getvalue())

    return {
        'statusCode': 200,
        'body': json.dumps('Data extracted successfully!')
    }

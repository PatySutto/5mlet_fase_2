import sys
import boto3
import logging
from pyspark.sql.types import DoubleType
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    avg, 
    col, 
    concat,
    current_date,
    date_format,
    lag,
    lpad,
    max,
    min,
    quarter,
    stddev,
    to_date,
    count,
    year, 
    month,
    sum as _sum,
    regexp_replace, 
    col
)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 1. Ler os dados parquet particionados da pasta raw
input_path = "s3://bucket-bovespa-653795152707/raw/"

df = spark.read.option("recursiveFileLookup", "true").parquet(input_path)

# obtendo o ano e mês corrente
current_year = date_format(current_date(), "yyyy").cast("int")
current_month = date_format(current_date(), "MM").cast("int")

# Obten apenas as colunas de interesse
colunas_de_interesse = ["cod", "asset", "type", "part", "theoricalQty", "pregao_date"]
df_filtrado = df.select(*colunas_de_interesse)


# renomeia as colunas originais
df_filtrado = df_filtrado.withColumnRenamed("cod", "codigo") \
        .withColumnRenamed("asset", "acao") \
        .withColumnRenamed("type", "tipo") \
        .withColumnRenamed("part", "part_porcentagem") \
        .withColumnRenamed("theoricalQty", "qtde_teorica") \
        .withColumnRenamed("pregao_date", "pregao_data")

# # Extrai ano e mês. Aux para fazer o agrupamento
# df_filtrado = df_filtrado.withColumn("pregao_ano", year("pregao_data")) \
#                          .withColumn("pregao_mes", month("pregao_data"))

# Define a janela por acao, tipo, ano e mês
janela = Window.partitionBy("acao", "tipo", "pregao_data")

# Cria nova coluna com a contagem por grupo
df_filtrado = df_filtrado.withColumn("frequencia_acao_tipo", count("*").over(janela))

# Converte coluna qtde_teorica para double
df_filtrado = df_filtrado.withColumn("qtde_teorica", col("qtde_teorica").cast("double"))
       
# Janela particionada por acao
janela_soma = Window.partitionBy("acao")

# Cria nova coluna com a soma total da qtde_teorica por acao
df_filtrado = df_filtrado.withColumn("total_qtde_teorica", _sum("qtde_teorica").over(janela_soma))

# cria a coluna de partição no formato "yyyy-MM-dd"
df_filtrado = df_filtrado.withColumn("dataproc", date_format(current_date(), "yyyy-MM-dd"))

# Trata texto para deixar no formato correto de número
df_filtrado = df_filtrado.withColumn(
    "part_porcentagem",
    regexp_replace(col("part_porcentagem"), ",", ".")
)

# Normaliza tipo das colunas 
df_filtrado = df_filtrado.withColumn("part_porcentagem", col("part_porcentagem").cast(DoubleType()))
df_filtrado = df_filtrado.withColumn("pregao_data", to_date(col("pregao_data")))

# cria o valor da partição no formato desejado
partition_value = df_filtrado.select("dataproc").first()["dataproc"]
partition_folder = f"bovespa_refined_data_{partition_value}"

# caminho completo com partição no nome da pasta
refined_data_path = f"s3://bucket-bovespa-653795152707/refined/{partition_folder}"

# salva os dados sem usar 'partitionBy', pois o nome da partição já está no path
df_filtrado.write.mode("overwrite").partitionBy("dataproc").parquet(refined_data_path)


## ----------------

# cliente do boto3 para Glue
glue_client = boto3.client("glue")

# define o nome do database e tabela para os dados transformados
database_name = "bovespa_db"
transformed_table_name = "tb_refined_data"
transformed_table_location = refined_data_path

# verifica se o banco de dados existe e cria se necessário
try:
    glue_client.get_database(Name=database_name)
except glue_client.exceptions.EntityNotFoundException:
    glue_client.create_database(
        DatabaseInput={'Name': database_name}
    )
    logger.info(f"Banco de dados '{database_name}' criado no Glue Catalog.")


# definição da tabela transformada com partição
transformed_table_input = {
    'Name': transformed_table_name,
    'StorageDescriptor': {
        'Columns': [
            {"Name": "codigo", "Type": "string"},
            {"Name": "acao", "Type": "string"},
            {"Name": "tipo", "Type": "string"},
            {"Name": "part_porcentagem", "Type": "double"},
            {"Name": "qtde_teorica", "Type": "double"},
            {"Name": "pregao_data", "Type": "date"},
            {"Name": "frequencia_acao_tipo", "Type": "int"},
            {"Name": "total_qtde_teorica", "Type": "double"}
        ],
        'Location': transformed_table_location,
        'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
        'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
        'Compressed': False,
        'SerdeInfo': {
            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
            'Parameters': {'serialization.format': '1'}
        }
    },
    'PartitionKeys': [{"Name": "dataproc", "Type": "string"}],  # Particionando por dataproc
    'TableType': 'EXTERNAL_TABLE'
}

# cria ou atualiza a tabela transformada no Glue Catalog
try:
    glue_client.get_table(DatabaseName=database_name, Name=transformed_table_name)
    logger.info(f"Tabela '{transformed_table_name}' já existe no Glue Catalog. Atualizando tabela...")
    glue_client.update_table(DatabaseName=database_name, TableInput=transformed_table_input)
    logger.info(f"Tabela '{transformed_table_name}' atualizada no Glue Catalog.")
except glue_client.exceptions.EntityNotFoundException:
    glue_client.create_table(DatabaseName=database_name, TableInput=transformed_table_input)
    logger.info(f"Tabela '{transformed_table_name}' criada no Glue Catalog.")

logger.info(f"Tabela '{transformed_table_name}' disponível no Athena.")

# executa MSCK REPAIR TABLE para descobrir partições
repair_table_query = f"MSCK REPAIR TABLE {database_name}.{transformed_table_name}"
logger.info(f"Executando comando: {repair_table_query}")
spark.sql(repair_table_query)
logger.info(f"Comando MSCK REPAIR TABLE executado com sucesso para a tabela '{transformed_table_name}'.")


job.commit()
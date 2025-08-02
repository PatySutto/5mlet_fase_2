import boto3

glue = boto3.client('glue')

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # # Divide a key para separar pasta e arquivo
    parts = key.rsplit('/', 2)  # Divide a partir da última barra
    particao_data = parts[1] if len(parts) > 1 else ''
    
    # Print para depuração
    print(f"Bucket: {bucket}")
    print('particao_data: ', particao_data)
   
    # Monta o caminho completo do arquivo S3
    s3_path = f"s3://{bucket}/{key}"

    print("Completo -->> ", s3_path)
    
    # Aqui você pode passar o parâmetro para o job Glue, se o job estiver configurado para receber parâmetros
    response = glue.start_job_run(
        JobName='transform_data',
        Arguments={
            '--raw_data': s3_path,  # parâmetro customizado
            '--particao_data': particao_data
        }
    )
    
    print(f"****** Started Glue job: {response['JobRunId']}")
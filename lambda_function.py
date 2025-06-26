import json  # librería para manipular json files
import boto3  # librería oficial de AWS para Python

def lambda_handler(event, context):
    print("Lambda started")

    s3 = boto3.client('s3')

    # Obtener nombre del bucket y archivo subido
    bucket_source = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    print(f"Reading file from source bucket: {bucket_source}, key: {key}")

    # Leer contenido del archivo
    try:
        obj = s3.get_object(Bucket=bucket_source, Key=key)
        print("File retrieved from S3")
        content = obj['Body'].read().decode('utf-8')
        print("File content decoded")
        data = json.loads(content)
        print("JSON parsed successfully")
    except Exception as e:
        print(f"Error reading or parsing file: {e}")
        raise

    # Definir destino
    bucket_landing = 'first-landing-2025'
    key_landing = key.replace('.json', '_procesado.json')
    print(f"Preparing to write to landing bucket: {bucket_landing}, key: {key_landing}")

    # Guardar archivo procesado
    try:
        s3.put_object(Bucket=bucket_landing, Key=key_landing, Body=json.dumps(data))
        print("File written successfully to landing bucket")
    except Exception as e:
        print(f"Error writing to landing bucket: {e}")
        raise

    print("Lambda completed successfully")
    return {
        'statusCode': 200,
        'body': json.dumps(f'Archivo {key} procesado correctamente')
    }

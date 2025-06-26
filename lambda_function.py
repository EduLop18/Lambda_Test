import json #libraria para manipular json files
import boto3 #libreria oficial de AWS para python

def lambda_handler(event, context): #funcion principal que AWS Lambda ejecuta automaticamente cuando se activa el trigger
    s3 = boto3.client('s3')

    # Bucket y key del archivo subido
    bucket_source = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # Leer contenido del archivo
    obj = s3.get_object(Bucket=bucket_source, Key=key)
    content = obj['Body'].read().decode('utf-8')
    data = json.loads(content)

    # Agregar marca de procesamiento / Dio error
    #data['procesado'] = True

    # Nombre del bucket de destino
    bucket_landing = 'first-landing-2025'
    key_landing = key.replace('.json', '_procesado.json')

    # Guardar el nuevo archivo
    s3.put_object(Bucket=bucket_landing, Key=key_landing, Body=json.dumps(data))

    return {
        'statusCode': 200,
        'body': json.dumps(f'Archivo {key} procesado correctamente')
    }

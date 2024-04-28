import os
import sys
import logging
from datetime import datetime
import json

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# Constantes
BUCKET_NAME = 'jvitor-desafio'
INPUT_PATH = './data'
STG_LAYER = 'Raw'
ORIGIN = 'Local'
DATA_ATUAL = datetime.now()

# Logger configs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(module)s %(levelname)s: %(message)s',
    datefmt='%Y/%m/%d %I:%M:%S %p %z',  # ISO 8601
    handlers=[
        logging.FileHandler(
            f'./logs/{DATA_ATUAL.strftime("%Y-%m-%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def obj_format_csv(filename: str) -> str:
    # Armazenando informações sobre o dado
    _FORMAT = 'CSV'
    file_wo_extension = os.path.splitext(os.path.basename(filename))[0]
    _ESPEC_DADO = file_wo_extension.capitalize()
    data_string_formatada = DATA_ATUAL.strftime("%Y/%m/%d")

    # Formatando o path do arquivo de acordo com o padrão do Bucket
    s3_key = os.path.join(
        STG_LAYER,
        ORIGIN,
        _FORMAT,
        _ESPEC_DADO,
        data_string_formatada,
        filename)

    return s3_key


def s3_auth() -> boto3.client:
    logger.info('Autenticando na nuvem da AWS...')
    s3 = boto3.client('s3')
    try:
        s3.get_bucket_acl(Bucket=BUCKET_NAME)
    except (ClientError, NoCredentialsError, Exception) as e:
        logger.exception(e)
        sys.exit(1)
    else:
        return s3


def object_exists(s3: boto3.client, bucket_name: str, s3_key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket_name, Key=s3_key)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        # Outros erros na resposta
        logger.warning(e)
        return False
    except Exception as e:
        logger.warning(e)
        return False
    else:
        logger.warning(f'[{s3_key}] - Objeto já existe')
        return True


def list_bucket_objs(s3: boto3.client, bucket_name: str) -> None:
    logger.info(f'Lista de objetos no bucket "{bucket_name}":')
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            objs = json.dumps(response['Contents'], default=str)
    except (ClientError, Exception) as e:
        logger.warning(e)
    else:
        logger.info(objs)


def try_upload(s3: boto3.client, **kwargs) -> None:
    try:
        s3.upload_file(
            kwargs['file_path'],
            kwargs['bucket_name'],
            kwargs['s3_key'])
    except (ClientError, Exception) as e:
        logger.exception(f'[{kwargs['arquivo']}] - {e}')
    else:
        logger.info(f'[{kwargs['arquivo']}] - Upload concluído com sucesso!')


def main() -> None:
    s3 = s3_auth()

    logger.info('Iniciando upload de arquivos...')
    for arquivo in os.listdir(INPUT_PATH):
        # Verificar se o arquivo não é CSV e passar para o próximo
        if not arquivo.endswith('.csv'):
            continue

        file_path = os.path.join(INPUT_PATH, arquivo)
        s3_key = obj_format_csv(arquivo)

        # Upload do arquivo
        logger.info(f'[{arquivo}] - Fazendo o upload')
        # Checando a existência do objeto no Bucket
        if object_exists(s3, BUCKET_NAME, s3_key):
            continue

        try_upload(
            s3,
            file_path=file_path,
            bucket_name=BUCKET_NAME,
            s3_key=s3_key,
            arquivo=arquivo)

    # Listando objects no bucket
    list_bucket_objs(s3, BUCKET_NAME)

    logger.info('################## FIM ##################')


if __name__ == '__main__':
    main()

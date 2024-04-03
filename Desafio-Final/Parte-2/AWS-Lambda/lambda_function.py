import sys
import os
import csv
import asyncio
from datetime import datetime
from typing import List, Callable
from base64 import b64decode
from io import StringIO

import boto3
import requests
import pandas as pd
import numpy as np

# Desencripta a chave de API
ENCRYPTED = os.environ['TMDB_ACCESS_TOKEN']
# Decrypt code should run once and variables stored outside of the function
# handler so that these are decrypted once per container
DECRYPTED = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(ENCRYPTED),
    EncryptionContext={
        'LambdaFunctionName': os.environ['AWS_LAMBDA_FUNCTION_NAME']}
)['Plaintext'].decode('utf-8')

STG_LAYER = 'Raw'
ORIGIN = 'TMDB'
FORMAT = 'JSON'
DATA_STRING = datetime.now().strftime("%Y/%m/%d")

# Client S3 Global
s3_client = boto3.client('s3')


def error_handling(func: Callable) -> None:  # Exception handling decorator
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (FileNotFoundError, PermissionError, UnicodeDecodeError) as e:
            print(f'{func.__name__} - {e}')
            sys.exit(1)
        except requests.exceptions.RequestException as e:
            url = kwargs.get('url')
            print(f'{func.__name__} - Falha ao solicitar URL <{url}>: {e}')
        except (IOError, TypeError, UnicodeEncodeError, csv.Error) as e:
            print(f'{func.__name__} - Erro na escrita do arquivo CSV: {e}')
        except Exception as e:
            print(e)
    return wrapper


@error_handling
def imdb_list_ids(bucket_name: str, file_path: str) -> List[str]:
    """Função imdb_list_ids
    Listar os IDs do arquivo CSV original que são dos gêneros Sci-Fi/Fantasia
    """
    lista_ids = []

    # Recuperar o objeto CSV
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_path)
    csv_data = obj['Body'].read().decode('utf-8')

    print(f'Listando as IDs dos filmes em {file_path.split('/')[-1]}')
    csv_reader = csv.reader(StringIO(csv_data), delimiter='|')

    for registro in csv_reader:
        genres = registro[5].split(',')
        if 'Sci-Fi' in genres or 'Fantasy' in genres:
            try:
                lista_ids.append(registro[0])
            except IndexError as e:
                # Lidar com registros vazios ou malformatados
                print(e)

    lista_ids = list(set(lista_ids))
    print(f'{len(lista_ids)} IDs distintos encontrados')

    return lista_ids


@error_handling
def salvar_detalhes_json(bucket_name: str, lista_detalhes: List[dict]) -> None:
    """Função salvar_detalhes_json
    Transforma a lista de detalhes em um DataFrame e salva os detalhes dos
    filmes encontrados na API em arquivos JSON particionados no Amazon S3
    """
    # Criando a estrutura de diretórios dentro do bucket
    s3_path_key = os.path.join(
        STG_LAYER,
        ORIGIN,
        FORMAT,
        DATA_STRING)
    try:
        s3_client.put_object(Bucket=bucket_name, Key=(s3_path_key + '/'))
    except Exception as e:
        print(f"Erro ao criar diretório no S3: {e}")

    df = pd.DataFrame(lista_detalhes)

    # Calcular o número de arquivos com no máximo 100 registros cada
    num_files = len(df) // 100 + 1

    # Dividir o DataFrame em partes
    dfs_split: List[pd.DataFrame] = []
    for df_partition in np.array_split(df, num_files):
        dfs_split.append(df_partition)

    # Salvar cada parte em um arquivo JSON no Amazon S3
    for i, df_partition in enumerate(dfs_split):
        output_file_key = f'{s3_path_key}/details-part-{i}.json'
        s3_obj_body = df_partition.to_json(
            orient='records', lines=True, force_ascii=False).encode('utf-8')

        # Enviar o arquivo JSON para o Amazon S3
        s3_client.put_object(Bucket=bucket_name, Key=output_file_key,
                             Body=s3_obj_body)

    print(f'Detalhes dos filmes salvos em s3://{bucket_name}/{s3_path_key}')


@error_handling
async def get_movies(id_list: List[str],
                     headers: dict,
                     num_requests: int = 500,
                     batch_size: int = 50) -> List[dict]:
    """Função get_movies
    Esta função é responsável por recuperar detalhes dos filmes da API
    The Movie Database (TMDb) e armazená-los em uma lista.

    Args:
        id_list (List[str]): Lista de ids de filmes do IMDB
        headers (dict): Headers do request a API
        num_requests (int, optional): Auto explicativo. Defaults to 500.
        batch_size (int, optional): Auto explicativo. Defaults to 50.

    Limitações:
    O número de requisições é limitado a 50 por segundo devido ao
    rate limiting da API. Documentação oficial em:
    https://developer.themoviedb.org/docs/rate-limiting

    Retorno:
        tmdb_details: Lista com dados filtrados do endpoint /movie/{movie_id}.
    """

    error_list: List[str] = []
    tmdb_details: List[dict] = []
    num_batches = (num_requests + batch_size - 1) // batch_size
    chaves_desejadas = [  # Filtro das chaves necessárias na resposta da API
        'id',
        'imdb_id',
        'title',
        'release_date',
        'vote_average',
        'vote_count',
        'popularity',
        'budget',
        'revenue',
        'runtime',
        'genres']

    async def get_movie_details(imdb_id: int) -> bool:
        url = f'https://api.themoviedb.org/3/movie/{imdb_id}'
        try:
            response = await asyncio \
                .to_thread(requests.get, url, headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException:
            error_list.append(imdb_id)
            return False
        else:
            registro = response.json()
            registro_filtrado = {
                chave: registro[chave]
                if chave in registro else None
                for chave in chaves_desejadas}

            tmdb_details.append(registro_filtrado)
            return True

    # Divide os lotes em tarefas com as requests assíncronas
    print('Recuperando dados do TMDB')
    current_batch = 1
    for i in range(0, num_requests, batch_size):
        batch_ids = id_list[i:i + batch_size]
        tasks = [get_movie_details(id) for id in batch_ids]

        # Aguardar a conclusão das tarefas antes de processar os resultados
        await asyncio.gather(*tasks)

        print(f'Batch processado | [{current_batch}/{num_batches}]')
        current_batch += 1
        # Esperar um segundo entre lotes por conta do rate limit da API
        await asyncio.sleep(1)

    if error_list:
        success_rate = (num_requests - len(error_list)) / num_requests
        print(f'Taxa de sucesso: {100 * success_rate:.2f}%')

    return tmdb_details


async def main(bucket_name, imdb_file_path) -> None:
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {DECRYPTED}"
    }

    id_list: List[str] = imdb_list_ids(bucket_name, imdb_file_path)
    detalhes_filmes: List[dict] = await get_movies(
        id_list,
        headers,
        num_requests=len(id_list)
    )

    salvar_detalhes_json(bucket_name, detalhes_filmes)


def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    imdb_file_path = event['Records'][0]['s3']['object']['key']
    asyncio.run(main(bucket_name, imdb_file_path))

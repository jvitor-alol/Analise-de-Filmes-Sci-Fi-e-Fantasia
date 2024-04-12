import sys
import os
import csv
import asyncio
import logging
from datetime import datetime
from typing import List, Callable

import requests
import pandas as pd
import numpy as np

# Constants
API_ACCESS_TOKEN = os.getenv('TMDB_ACCESS_TOKEN')
IMDB_FILE_PATH = './data/movies.csv'
BUCKET_NAME = 'jvitor-desafio'
STG_LAYER = 'Raw'
ORIGIN = 'TMDB'
DATA_ATUAL = datetime.now()

# Logger configs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(module)s %(levelname)s: %(message)s',
    datefmt='%Y/%m/%d %I:%M:%S %p %z',  # ISO 8601
    handlers=[
        logging.FileHandler(
            f'./logs/{datetime.now().strftime("%Y-%m-%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def error_handling(func: Callable) -> None:  # Exception handling decorator
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (FileNotFoundError, PermissionError, UnicodeDecodeError) as e:
            logger.exception(f'{func.__name__} - {e}')
            sys.exit(1)
        except requests.exceptions.RequestException as e:
            url = kwargs.get('url')
            logger.exception(
                f'{func.__name__} - Falha ao solicitar URL <{url}>: {e}')
        except (IOError, TypeError, UnicodeEncodeError, csv.Error) as e:
            logger.exception(
                f'{func.__name__} - Erro na escrita do arquivo CSV: {e}')
        except Exception as e:
            logger.exception(e)
    return wrapper


@error_handling
def imdb_list_ids(file_path: str) -> List[str]:
    """Função imdb_list_ids
    Listar os IDs do arquivo CSV original que são dos gêneros Sci-Fi/Fantasia
    """
    lista_ids = []

    logger.info(f'Listando as IDs dos filmes em {file_path.split('/')[-1]}')
    with open(file_path, mode='r', encoding='utf-8') as arquivo:
        registros = csv.reader(arquivo, delimiter='|')
        for registro in registros:
            genres = registro[5].split(',')
            if 'Sci-Fi' in genres or 'Fantasy' in genres:
                try:
                    lista_ids.append(registro[0])
                except IndexError as e:
                    # Lidar com registros vazios ou malformatados
                    logger.warning(e)

    lista_ids = list(set(lista_ids))
    logger.info(f'{len(lista_ids)} IDs distintos encontrados')

    return lista_ids


@error_handling
def salvar_lista_erros(lista: List[str]) -> None:
    """Função salvar_lista_erros
    Salva uma lista com os IDs que não forem encontrados no TMDB em um
    arquivo CSV
    """
    output_path = './output/erros.csv'
    with open(output_path, mode='w', newline='') as arquivo_csv:
        escritor_csv = csv.writer(arquivo_csv)
        for item in lista:
            escritor_csv.writerow([item])

    logger.warning(f'IDs sem detalhes salvos em {output_path}')


@error_handling
def salvar_detalhes_json(lista_detalhes: List[dict]) -> None:
    """Função salvar_detalhes_json
    Transforma a lista de detalhes em um DataFrame e salva os detalhes dos
    filmes encontrados na API em arquivos JSON particionados
    """
    output_path = './output/tmdb_movies'

    df = pd.DataFrame(lista_detalhes)

    # Calcular o número de arquivos com no máximo 100 registros cada
    num_files = len(df) // 100 + 1

    # Dividir o DataFrame em partes
    dfs_split: List[pd.DataFrame] = []
    for df_partition in np.array_split(df, num_files):
        dfs_split.append(df_partition)

    # Salvar cada parte em um arquivo JSON
    for i, df_partition in enumerate(dfs_split):
        output_file = os.path.join(output_path, f'details-part-{i}.json')
        with open(output_file, mode='w', encoding='utf-8') as file:
            df_partition.to_json(
                file,
                orient='records',
                lines=True,
                force_ascii=False)

    logger.info(f'Detalhes dos filmes salvos em {output_path}')


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
            # logger.warning(f'ID não encontrado - {imdb_id:>10} | {e}')
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
    logger.info('Recuperando dados do TMDB')
    current_batch = 1
    for i in range(0, num_requests, batch_size):
        batch_ids = id_list[i:i + batch_size]
        tasks = [get_movie_details(id) for id in batch_ids]

        # Aguardar a conclusão das tarefas antes de processar os resultados
        await asyncio.gather(*tasks)

        logger.info(f'Batch processado | [{current_batch}/{num_batches}]')
        current_batch += 1
        # Esperar um segundo entre lotes por conta do rate limit da API
        await asyncio.sleep(1)

    # Se ao menos um ID não for encontrado a lista de erros é salva em CSV
    if error_list:
        success_rate = (num_requests - len(error_list)) / num_requests
        logger.warning(f'Taxa de sucesso: {100 * success_rate:.2f}%')
        salvar_lista_erros(error_list)

    return tmdb_details


async def main() -> None:
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {API_ACCESS_TOKEN}"
    }

    id_list: List[str] = imdb_list_ids(IMDB_FILE_PATH)
    detalhes_filmes: List[dict] = await get_movies(
        id_list,
        headers,
        # num_requests=len(id_list)
    )

    salvar_detalhes_json(detalhes_filmes)


if __name__ == '__main__':
    asyncio.run(main())

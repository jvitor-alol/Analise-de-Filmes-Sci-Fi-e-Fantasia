import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import Window
from awsglue.context import GlueContext
from awsglue.job import Job

# @params: [JOB_NAME]
args = getResolvedOptions(
    sys.argv, ['JOB_NAME', 'TMDB_DATA_PATH',
               'IMDB_DATA_PATH', 'S3_TARGET_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

tmdb_path = args['TMDB_DATA_PATH']
imdb_path = args['IMDB_DATA_PATH']
output_path = args['S3_TARGET_PATH']

# Leitura dos dados do S3 e manipulação dos DataFrames
df_tmdb = spark.read.parquet(tmdb_path)
df_imdb = spark.read.parquet(imdb_path)

df_imdb = df_imdb \
    .withColumn('id_genre',
                F.when(df_imdb['genero'].like(
                    '%Fantasy%') & ~df_imdb['genero'].like('%Sci-Fi%'), 1)
                .when(df_imdb['genero'].like(
                    '%Sci-Fi%') & ~df_imdb['genero'].like('%Fantasy%'), 2)
                .when(df_imdb['genero'].like(
                    '%Fantasy%') & df_imdb['genero'].like('%Sci-Fi%'), 3)
                .otherwise(None))
df_tmdb = df_tmdb.withColumnRenamed('id', 'tmdb_id')
df_tmdb.printSchema()
df_imdb.printSchema()

# View para facilitar consultas SQL
df_geral = df_imdb.join(df_tmdb, df_imdb.id == df_tmdb.imdb_id, 'inner')
df_geral.createOrReplaceTempView('db_geral')
df_geral.printSchema()
df_geral.describe().show(truncate=False)

print(spark.sql('SELECT DISTINCT id FROM db_geral').count())  # 1316

# Criando e salvando tabela de artistas
df_actors = df_imdb.groupBy('nomeArtista').count()
df_actors = df_actors.withColumnRenamed('nomeArtista', 'name') \
    .withColumnRenamed('count', 'num_movies')

# Add coluna de id
df_actors = df_actors.select(
    F.row_number().over(Window.orderBy(F.lit(1))).alias('id'),
    *df_actors.columns)

df_actors.printSchema()
df_actors.describe().show()
df_actors.show(5)

df_actors.write.mode('overwrite').parquet(output_path + 'dim_actor')

# Criando e salvando tabela de datas
df_dates = df_tmdb.select(df_tmdb.release_date.alias('date')) \
    .distinct().orderBy('release_date')
df_dates = df_dates.withColumn('year', F.year('date')) \
    .withColumn('month', F.month('date')) \
    .withColumn('day', F.dayofmonth('date')) \
    .withColumn('quarter',
                F.when(F.month('date').isin([1, 2, 3]), 'Q1')
                .when(F.month('date').isin([4, 5, 6]), 'Q2')
                .when(F.month('date').isin([7, 8, 9]), 'Q3')
                .otherwise('Q4'))

# Add coluna de id
df_dates = df_dates.select(
    F.row_number().over(Window.orderBy(F.lit(1))).alias('id'),
    *df_dates.columns)

df_dates.printSchema()
df_dates.describe().show()
df_dates.show(5)

df_dates.write.mode('overwrite').parquet(output_path + 'dim_date')

# Criando e salvando tabela de filmes
df_movies = spark.sql("""
    SELECT DISTINCT
        id,
        tituloPrincipal AS main_title,
        tituloOriginal AS original_title,
        release_date,
        genero AS genres
    FROM db_geral
""")

df_movies.printSchema()
df_movies.describe().show()
df_movies.show(5)

df_movies.write.mode('overwrite').parquet(output_path + 'dim_movie')

# Criando e salvando tabela fato
df_fato = spark.sql("""
    SELECT DISTINCT
        id AS id_movie,
        nomeArtista,
        release_date,
        id_genre,
        runtime,
        revenue,
        budget,
        personagem AS actor_role,
        popularity,
        notaMedia AS imdb_rating,
        vote_average AS tmdb_rating,
        numeroVotos AS imdb_vote_count,
        vote_count AS tmdb_vote_count
    FROM db_geral
""")

# Add chaves estrangeiras
df_fato = df_fato.join(df_dates,
                       df_fato.release_date == df_dates.date, 'inner')
df_fato = df_fato.withColumnRenamed('id', 'id_date')
df_fato = df_fato.join(df_actors,
                       df_fato.nomeArtista == df_actors.name, 'inner')
df_fato = df_fato.withColumnRenamed('id', 'id_actor')

# Eliminando as colunas indesejadas sem alterar os dados de IDs estrangeiros
colunas_indesejadas = ['release_date', 'date', 'year', 'month', 'day',
                       'quarter', 'nomeArtista', 'name', 'num_movies']
df_pandas = df_fato.toPandas()  # DataFrame Pandas é imutável
df_pandas = df_pandas.drop(colunas_indesejadas, axis=1)
df_fato = spark.createDataFrame(df_pandas)

# Reordenando as colunas
df_fato = df_fato.select(
    *df_fato.columns[:2], *df_fato.columns[-2:], *df_fato.columns[2:-2])

df_fato.printSchema()
df_fato.describe().show()
df_fato.orderBy('id_movie').show()

df_fato.write.partitionBy('id_movie').mode('overwrite') \
    .parquet(output_path + 'fact_movie_actor')

# Criando e salvando tabela de gêneros
df_genres = df_fato.select('id_movie', 'id_genre').distinct() \
    .groupBy('id_genre').count()
df_genres = df_genres.withColumnRenamed('id_genre', 'id') \
    .withColumnRenamed('count', 'num_movies')\
    .withColumn('genre',
                F.when(F.col('id') == 1, 'Fantasy')
                .when(F.col('id') == 2, 'Sci-Fi')
                .when(F.col('id') == 3, 'Fantasy/Sci-Fi'))

df_genres = df_genres.select('id', 'genre', 'num_movies').orderBy('id')
df_genres.printSchema()
df_genres.describe().show()
df_genres.show(5)

df_genres.write.mode('overwrite').parquet(output_path + 'dim_genre')

job.commit()

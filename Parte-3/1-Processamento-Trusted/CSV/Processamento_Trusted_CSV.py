import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.job import Job

# @params: [JOB_NAME]
args = getResolvedOptions(
    sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_TARGET_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

source_file = args['S3_INPUT_PATH']
output_path = args['S3_TARGET_PATH']

# Definição do schema, leitura do csv e verificação de dados
schema = """id STRING,
            tituloPincipal STRING,
            tituloOriginal STRING,
            anoLancamento INT,
            tempoMinutos INT,
            genero STRING,
            notaMedia DOUBLE,
            numeroVotos INT,
            generoArtista STRING,
            personagem STRING,
            nomeArtista STRING,
            anoNascimento INT,
            anoFalecimento INT,
            profissao STRING,
            titulosMaisConhecidos STRING
            """
df = spark.read.option("encoding", "UTF-8") \
    .csv(source_file, schema=schema, sep="|", header=True)
df.printSchema()
print(df.count())  # 1045161 registros

# Manipulação de colunas
colunas_indesejadas = ['generoArtista', 'anoNascimento',
                       'anoFalecimento', 'profissao', 'titulosMaisConhecidos']
df = df.drop(*colunas_indesejadas)
df = df.withColumnRenamed('tituloPincipal', 'tituloPrincipal')

# Filtrando os gêneros desejados
df = df.filter(F.col('genero').like('%Sci-Fi%') |
               F.col('genero').like('%Fantasy%'))
df.printSchema()
print(df.count())  # 61569 registros
print(df.select('id').distinct().count())  # Num de IDs distintos = 14303

# Eliminando registros duplicados e dados incompletos
df_novo = df.dropDuplicates()
df_novo = df_novo.dropna()
df.show()
print(df_novo.count())  # 57520

# Substituindo '\N' por valores nulos na coluna 'personagem'
# Esses valores não atrapalham a analise então não serão deletados
df_novo = df_novo.withColumn(
    'personagem', F.when(F.col('personagem') == '\\N', None)
    .otherwise(F.col('personagem')))

# Amostra de votos menor que 30 é considerada irrelevante (TCL)
df_novo = df_novo.filter(F.col("numeroVotos") >= 30)

# Verificando que não há inconsistências nos dados dos filmes
ids_distintos = df_novo.select('id').distinct().count()
print(ids_distintos)  # Resultado: 11081
ids_distintos = df_novo.select('id', 'tituloPrincipal', 'tituloOriginal',
                               'anoLancamento', 'tempoMinutos', 'genero',
                               'notaMedia', 'numeroVotos').distinct().count()
print(ids_distintos)  # Resultado: 11081

# Ultima checagagem dos dados
print(df_novo.count())  # 46835
df_novo.printSchema()
df_novo.describe().show()
df_novo.show()

df_novo.write.parquet(output_path)

job.commit()

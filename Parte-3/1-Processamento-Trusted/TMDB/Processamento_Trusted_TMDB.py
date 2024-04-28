import sys
from datetime import datetime
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

# Leitura dos json e verificação de dados
df = spark.read.option("encoding", "UTF-8").json(source_file)
print(df.count())
df.printSchema()
df.show()

# Remove dados duplicados se houver
df = df.dropDuplicates()
print(df.count())

# Drop coluna de gêneros, e cast no release_date para date
df = df.drop("genres")
df = df.withColumn('release_date', F.to_date(df['release_date'], 'yyyy-MM-dd'))
df.printSchema()

# Filmes com receita, orçamento ou duração zerados serão
# desconsiderados como valores nulos
df_novo = df.filter(
    (F.col("revenue") != 0) & (F.col("budget") != 0) & (F.col("runtime") != 0))

# Filmes com menos de 30 votos serão considerados como amostra insuficiente (TCL)
df_novo = df_novo.filter(F.col("vote_count") >= 30)

# Data de coleta a partir da pasta lida
partes = source_file.split("/")

# A última parte do caminho é a data no formato "YYYY/MM/DD"
ano = partes[-4]
mes = partes[-3]
dia = partes[-2]
data = datetime.strptime(f"{ano}-{mes}-{dia}", "%Y-%m-%d").date()

df_novo = df_novo.withColumn("extraction_date", F.lit(data))

# Ultima checagagem dos dados
print(df_novo.count())
df_novo.printSchema()
df_novo.describe().show()
df_novo.show()

df_novo.write.partitionBy("extraction_date").parquet(output_path)

job.commit()

## Código pasa uso no AWS Glue

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
job.commit()

from awsglue.context import GlueContext
from pyspark.context import SparkContext
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from delta.tables import *
logger = glueContext.get_logger()
# create/get spark + glue context
spark = GlueContext(SparkContext.getOrCreate()).sparkSession

# folder within S3 for the delta table
logger.info('Iniciando execucao')
# folder within S3 for the delta table

def criarDF():
    s3_path = f"s3://delta-glue-da/user_delta"
    # initially prepopulate the table with some data
    users_initial = [
        { 'user_id': 1, 'name': 'Gina Burch', 'gender': 'f' },
        { 'user_id': 2, 'name': 'Francesco Coates', 'gender': 'm' },
        { 'user_id': 3, 'name': 'Saeed Wicks', 'gender': 'm' },
        { 'user_id': 4, 'name': 'Raisa Oconnell', 'gender': 'f' },
        { 'user_id': 5, 'name': 'Josh Copeland', 'gender': 'm' },
        { 'user_id': 6, 'name': 'Kaiden Williamson', 'gender': 'm' }
    ]
    logger.info('Criando df')
    df = spark.createDataFrame(users_initial)
    logger.info('Salvando no S3')
    df.write.format("delta").mode("overwrite").partitionBy("user_id").save(s3_path)
    logger.info('Salvo com sucesso')

def lerDF():
    s3_path = f"s3://delta-glue-da/user_delta/"
    s3_path_csv = f"s3://delta-glue-da/user_delta_para_csv/"
    logger.info("Lendo DF:")
    df = spark.read.format("delta").load(s3_path).orderBy("user_id")
    logger.info('Transformando e salvando em csv')
    logger.info('Salvando em CSV')
    df.write.format("csv").option("header",True).option("delimiter",";").mode("overwrite").partitionBy("user_id").save(s3_path_csv)
    logger.info('Salvou em CSV')

def alterarDeltaLake():
    s3_path = f"s3://delta-glue-da/user_delta/"
    s3_path_csv = f"s3://delta-glue-da/user_delta_para_csv/"
    logger.info('Consultando dados de Delta')
    deltaTable = DeltaTable.forPath(spark, s3_path)
    logger.info('Alterando dados de Delta')
    deltaTable.update(
        condition = col('user_id') == 2,
        set = { "name": "'Bruno Sales'"})
    logger.info("Alterado com sucesso")
    df = deltaTable.toDF().orderBy("user_id")
    logger.info('Salvando em CSV')
    df.write.format("csv").option("header",True).option("delimiter",";").mode("overwrite").partitionBy("user_id").save(s3_path_csv)
    logger.info('Salvou em CSV')

def deletarRegistro():
    s3_path = f"s3://delta-glue-da/user_delta/"
    s3_path_csv = f"s3://delta-glue-da/user_delta_para_csv/"
    logger.info('Consultando dados de deltaLake')
    deltaTable = DeltaTable.forPath(spark, s3_path)
    print('Deletando registro')
    deltaTable.delete("user_id = 5")
    print('Registro deletado com sucesso')
    df = deltaTable.toDF().orderBy("user_id")
    logger.info('Salvando em CSV')
    df.write.format("csv").option("header",True).option("delimiter",";").mode("overwrite").partitionBy("user_id").save(s3_path_csv)
    logger.info('Salvou em CSV')

alterarDeltaLake()

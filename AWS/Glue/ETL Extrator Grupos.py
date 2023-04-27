import boto3
import datetime
import json
import pandas as pd
import pyspark
import sys
import traceback

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions

from delta import *
from delta.tables import *

from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

logger = glueContext.get_logger()
job.commit()

def arquivos_json():
    try:
        s3_path = f"s3://bucket-teste/projeto/silver/user"
        AWS_REGION = "us-east-1"
        S3_BUCKET_NAME = 'bucket-teste'
        s3_resource = boto3.resource("s3", region_name=AWS_REGION)
        s3_bucket = s3_resource.Bucket(S3_BUCKET_NAME)
        logger.info(f"Bucket: {s3_bucket}")
        df_final = pd.DataFrame()
        contador = 0
        for obj in s3_bucket.objects.filter(Prefix='projeto/bronze/grupo/pendentes/'):
            logger.info(f'Arquivo para extrair json: {obj}')
            file_content = obj.get()['Body'].read().decode('utf-8')
            json_content = json.loads(file_content)
            df = pd.DataFrame(json_content['results'])
            df_final = pd.concat([df,df_final])
            contador += 1
            if contador == 2:
                break
        df = df_final
        return df
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        logger.error('Erro: '+str(e)+' na linha '+str(exc_tb.tb_lineno))
        return False

def grupo():
    try:
        s3_path= f"s3://bucket-teste/projeto/silver/grupo/"
        df = arquivos_json()
        df['Year']= pd.to_datetime(df['updated_at']).dt.year
        logger.info(f"DF pandas: {df.head()}")
        df[df.select_dtypes("object").columns] =df.select_dtypes(include='object').fillna('')
        df[df.select_dtypes("int64").columns] =df.select_dtypes(include='int64').fillna(0)
        df[df.select_dtypes("float64").columns] =df.select_dtypes(include='float64').fillna(0)
        df[df.select_dtypes("bool").columns] =df.select_dtypes(include='bool').fillna(False)
        logger.info('Sucesso Pandas')
        logger.info(f'resultado pandas: {df.head()}')
        logger.info(f'QTD DF pandas: {len(df)}')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        logger.error('Erro: '+str(e)+' na linha '+str(exc_tb.tb_lineno))
        return False


def executarETL():
    logger.info('Iniciando execucao')
    s3_path = f"s3://bucket-teste/projeto/silver/grupo/"
    s3_path_csv = f"s3://bucket-teste/projeto/silver/grupo/grupo_delta_para_csv/"
    bucket_name = 'bucket-teste'
    arquivos_json()
    grupo()
    #extrair_dados_s3()
    #exibirDadosS3(s3_path, s3_path_csv)
    logger.info('Finalizando execucao')

executarETL()
sc.stop()

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Importando seus módulos auxiliares
import extract
import validate
import load_file_silver_layer
import logger_config

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['our_first_pipeline'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['our_first_pipeline'], args)

# Configuração de logger
logger = logger_config.get_logger()

def main():
    logger.info("Iniciando pipeline ETL no Glue...")

    # 1. Extração dos dados brutos
    customers_df = extract.read_csv_from_s3(
        spark,
        bucket="source-bucket-230525",
        key="customers.csv"
    )
    items_df = extract.read_csv_from_s3(
        spark,
        bucket="source-bucket-230525",
        key="items.csv"
    )

    logger.info(f"Clientes lidos: {customers_df.count()} registros")
    logger.info(f"Itens lidos: {items_df.count()} registros")

    # 2. Validação dos dados
    customers_df = validate.clean_customers(customers_df)
    items_df = validate.clean_items(items_df)

    # 3. Carregar na camada Silver (Parquet particionado)
    load_file_silver_layer.write_parquet(
        df=customers_df,
        bucket="target-bucket-230525",
        prefix="customers"
    )
    load_file_silver_layer.write_parquet(
        df=items_df,
        bucket="target-bucket-230525",
        prefix="items"
    )

    logger.info("Pipeline ETL concluído com sucesso.")

if __name__ == "__main__":
    main()
    job.commit()

from pyspark.sql import SparkSession
import argparse

def parse_arguments():
    parser = argparse.ArgumentParser(description='Create TPCDS tables in S3Tables format')
    parser.add_argument('--source-dir', required=True,
                      help='Source directory containing TPCDS data (e.g., s3a://bucket/tpcds/1g/warehouse/parquet/)')
    parser.add_argument('--target-dir', required=True,
                      help='Target S3 directory for the tables (e.g., arn:aws:s3tables:region:account:bucket/tpcds1g)')
    parser.add_argument('--s3tables-name', required=True,
                      help='Name for the S3Tables catalog (e.g., tpcds1g)')
    return parser.parse_args()

def create_spark_session(target_dir):
    return SparkSession.builder \
        .appName("S3Tables Iceberg Job") \
        .config("spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.s3tablesbucket.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog") \
        .config("spark.sql.catalog.s3tablesbucket.warehouse", target_dir) \
        .config("spark.jars.packages", "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.3") \
        .getOrCreate()

def create_table(spark, table_name, source_dir, s3tables_name, partition_col=None):
    try:
        # Read the source data
        source_path = f"{source_dir.rstrip('/')}/{table_name}/"
        tpcds_df = spark.read.load(source_path)
        
        # Register the DataFrame as a temporary view
        tpcds_df.createOrReplaceTempView(table_name)
        
        # Create the Iceberg table using SQL
        partition_clause = f"PARTITIONED BY ({partition_col})" if partition_col else ""
        
        # change to 524288 for small data test, origin 536870912
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS s3tablesbucket.{s3tables_name}.{table_name}
        USING iceberg
        {partition_clause}
        TBLPROPERTIES (
            'write.object-storage.enabled'='true',
            'write.target-file-size-bytes'='524288'
        ) AS
        SELECT * FROM {table_name}
        """
        
        spark.sql(create_table_sql)
        print(f"Table {table_name} creation completed successfully")
        
    except Exception as e:
        print(f"An error occurred while creating {table_name}: {str(e)}")

def main():
    # Parse command line arguments
    args = parse_arguments()
    
    # Create Spark session
    spark = create_spark_session(args.target_dir)
    
    # Create namespace
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS s3tablesbucket.{args.s3tables_name}")
    
    # Define tables and their partition columns (if any)
    tables_config = {
        "call_center": None,
        "catalog_page": None,
        "catalog_returns": "cr_returned_date_sk",
        "catalog_sales": "cs_sold_date_sk",
        "customer": None,
        "customer_address": None,
        "customer_demographics": None,
        "date_dim": None,
        "household_demographics": None,
        "income_band": None,
        "inventory": "inv_date_sk",
        "item": None,
        "promotion": None,
        "reason": None,
        "ship_mode": None,
        "store": None,
        "store_returns": "sr_returned_date_sk",
        "store_sales": "ss_sold_date_sk",
        "time_dim": None,
        "warehouse": None,
        "web_page": None,
        "web_returns": "wr_returned_date_sk",
        "web_sales": "ws_sold_date_sk",
        "web_site": None
    }
    
    # Create all tables
    for table_name, partition_col in tables_config.items():
        create_table(
            spark=spark,
            table_name=table_name,
            source_dir=args.source_dir,
            s3tables_name=args.s3tables_name,
            partition_col=partition_col
        )

if __name__ == "__main__":
    main()
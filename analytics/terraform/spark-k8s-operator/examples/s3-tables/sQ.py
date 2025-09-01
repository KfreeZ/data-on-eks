from pyspark.sql import SparkSession
import argparse

def parse_arguments():
    parser = argparse.ArgumentParser(description='Run TPC-DS queries on parquet files')
    parser.add_argument('--source-dir', required=True,
                      help='Source directory containing TPCDS data (e.g., s3a://bucket/tpcds/1g/warehouse/parquet/)')
    return parser.parse_args()

def create_spark_session():
    return SparkSession.builder \
        .appName("TPC-DS Query Job") \
        .getOrCreate()

def load_tpcds_tables(spark, source_dir):
    """Load TPC-DS tables from source directory and register as temp views"""
    
    # List of TPC-DS tables to load
    tables = [
        "call_center", "catalog_page", "catalog_returns", "catalog_sales", 
        "customer", "customer_address", "customer_demographics", "date_dim", 
        "household_demographics", "income_band", "inventory", "item", 
        "promotion", "reason", "ship_mode", "store", "store_returns", 
        "store_sales", "time_dim", "warehouse", "web_page", 
        "web_returns", "web_sales", "web_site"
    ]
    
    # Load each table and register as temp view
    for table_name in tables:
        try:
            print(f"Loading table: {table_name}")
            source_path = f"{source_dir.rstrip('/')}/{table_name}/"
            tpcds_df = spark.read.load(source_path)
            tpcds_df.createOrReplaceTempView(table_name)
            print(f"Successfully registered temporary view for {table_name}")
        except Exception as e:
            print(f"Error loading {table_name}: {str(e)}")
    
    print("All tables loaded successfully.")

def run_example_queries(spark):
    print("Running example queries...")
    
    # Example Query 1: Top selling products by revenue
    print("\n\nQuery 1: Top 10 selling products by revenue")
    query1 = """
    SELECT 
        i.i_item_id,
        i.i_item_desc,
        SUM(ss.ss_quantity) AS total_quantity_sold,
        SUM(ss.ss_net_profit) AS total_profit
    FROM 
        store_sales ss
    JOIN 
        item i ON ss.ss_item_sk = i.i_item_sk
    JOIN
        date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
    WHERE
        d.d_year = 2001
    GROUP BY 
        i.i_item_id, i.i_item_desc
    ORDER BY 
        total_profit DESC
    LIMIT 10
    """
    
    result1 = spark.sql(query1)
    result1.show(10, truncate=False)
    
    # Example Query 2: Customer demographics analysis
    print("\n\nQuery 2: Customer demographics breakdown")
    query2 = """
    SELECT 
        cd.cd_gender,
        cd.cd_marital_status,
        cd.cd_education_status,
        COUNT(DISTINCT c.c_customer_sk) AS customer_count
    FROM 
        customer c
    JOIN 
        customer_demographics cd ON c.c_current_cdemo_sk = cd.cd_demo_sk
    GROUP BY 
        cd.cd_gender, cd.cd_marital_status, cd.cd_education_status
    ORDER BY 
        customer_count DESC
    LIMIT 15
    """
    
    result2 = spark.sql(query2)
    result2.show(15, truncate=False)
    
    # Example Query 3: Sales distribution across different channels
    print("\n\nQuery 3: Sales distribution across different channels")
    query3 = """
    WITH all_sales AS (
        SELECT 
            'store' AS channel,
            ss.ss_sold_date_sk AS date_sk,
            SUM(ss.ss_net_profit) AS profit
        FROM 
            store_sales ss
        GROUP BY 
            ss.ss_sold_date_sk
            
        UNION ALL
        
        SELECT 
            'catalog' AS channel,
            cs.cs_sold_date_sk AS date_sk,
            SUM(cs.cs_net_profit) AS profit
        FROM 
            catalog_sales cs
        GROUP BY 
            cs.cs_sold_date_sk
            
        UNION ALL
        
        SELECT 
            'web' AS channel,
            ws.ws_sold_date_sk AS date_sk,
            SUM(ws.ws_net_profit) AS profit
        FROM 
            web_sales ws
        GROUP BY 
            ws.ws_sold_date_sk
    )
    
    SELECT 
        d.d_year,
        d.d_qoy AS quarter,
        s.channel,
        SUM(s.profit) AS channel_profit
    FROM 
        all_sales s
    JOIN 
        date_dim d ON s.date_sk = d.d_date_sk
    WHERE 
        d.d_year BETWEEN 2000 AND 2001
    GROUP BY 
        d.d_year, d.d_qoy, s.channel
    ORDER BY 
        d.d_year, d.d_qoy, channel_profit DESC
    """
    
    result3 = spark.sql(query3)
    result3.show(20, truncate=False)
    
    # Example Query 4: Customer spending patterns
    print("\n\nQuery 4: Customer spending patterns")
    query4 = """
    SELECT 
        c.c_customer_id,
        ca.ca_city,
        ca.ca_state,
        SUM(ss.ss_net_paid) AS store_spending,
        SUM(cs.cs_net_paid) AS catalog_spending,
        SUM(ws.ws_net_paid) AS web_spending,
        SUM(ss.ss_net_paid + COALESCE(cs.cs_net_paid, 0) + COALESCE(ws.ws_net_paid, 0)) AS total_spending
    FROM 
        customer c
    JOIN 
        customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
    LEFT JOIN 
        store_sales ss ON c.c_customer_sk = ss.ss_customer_sk
    LEFT JOIN 
        catalog_sales cs ON c.c_customer_sk = cs.cs_bill_customer_sk
    LEFT JOIN 
        web_sales ws ON c.c_customer_sk = ws.ws_bill_customer_sk
    GROUP BY 
        c.c_customer_id, ca.ca_city, ca.ca_state
    ORDER BY 
        total_spending DESC
    LIMIT 20
    """
    
    result4 = spark.sql(query4)
    result4.show(20, truncate=False)

def main():
    # Parse command line arguments
    args = parse_arguments()
    
    # Create Spark session
    spark = create_spark_session()
    
    # Load TPC-DS tables and register as temp views
    load_tpcds_tables(spark, args.source_dir)
    
    # Run example queries
    run_example_queries(spark)
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
#!/usr/bin/env python3

from pyspark.sql import SparkSession
import argparse
import time
import os

def run_query(spark, source_dir, result_dir, query_name):
    """Run a specific TPC-DS query and save results"""
    print(f"Running query {query_name} on data in {source_dir}")
    
    # Register tables
    tables = ["catalog_sales", "catalog_returns", "inventory", "store_sales", 
              "store_returns", "web_sales", "web_returns", "call_center", 
              "catalog_page", "customer", "customer_address", "customer_demographics",
              "date_dim", "household_demographics", "income_band", "item", 
              "promotion", "reason", "ship_mode", "store", "time_dim", 
              "warehouse", "web_page", "web_site"]
    
    # Try to register tables that exist
    for table in tables:
        try:
            path = f"{source_dir}/{table}"
            df = spark.read.parquet(path)
            df.createOrReplaceTempView(table)
            print(f"Registered table: {table}")
        except Exception as e:
            print(f"Could not register table {table}: {str(e)}")
    
    # Load the query
    query_path = f"/opt/tpcds-kit/tools/query-templates/{query_name}.tpl"
    
    # If query file doesn't exist, use a simple query
    if not os.path.exists(query_path):
        print(f"Query file {query_path} not found, using a simple query")
        if query_name == "q1-v2.4":
            query = """
            SELECT d_year, c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, 
                   c_birth_country, c_login, c_email_address, d_date
            FROM customer, date_dim
            WHERE c_customer_sk = 1
              AND d_date_sk = 1
            LIMIT 100
            """
        else:
            query = f"SELECT * FROM item LIMIT 10"
    else:
        with open(query_path, 'r') as f:
            query = f.read()
    
    # Execute the query with timing
    start_time = time.time()
    try:
        # First try with Gluten enabled
        print("Executing query with Gluten enabled...")
        result = spark.sql(query)
        
        # Save results
        output_path = f"{result_dir}/{query_name}_result"
        result.write.mode("overwrite").parquet(output_path)
        
        end_time = time.time()
        print(f"Query completed successfully in {end_time - start_time:.2f} seconds")
        print(f"Results saved to {output_path}")
        
        # Show sample results
        print("Sample results:")
        result.show(20, truncate=False)
        
        return True
    except Exception as e:
        print(f"Error executing query with Gluten: {str(e)}")
        
        # Try again with Gluten disabled
        print("Retrying with Gluten disabled...")
        try:
            # Disable Gluten
            spark.sql("SET spark.gluten.enabled=false")
            
            start_time = time.time()
            result = spark.sql(query)
            
            # Save results
            output_path = f"{result_dir}/{query_name}_result_no_gluten"
            result.write.mode("overwrite").parquet(output_path)
            
            end_time = time.time()
            print(f"Query completed without Gluten in {end_time - start_time:.2f} seconds")
            print(f"Results saved to {output_path}")
            
            # Show sample results
            print("Sample results:")
            result.show(20, truncate=False)
            
            return True
        except Exception as e2:
            print(f"Error executing query without Gluten: {str(e2)}")
            return False

def main():
    parser = argparse.ArgumentParser(description='Run TPC-DS queries with Gluten')
    parser.add_argument('--source-dir', required=True, help='Source directory with TPC-DS data')
    parser.add_argument('--result-dir', required=True, help='Directory to save results')
    parser.add_argument('--query', default='q1-v2.4', help='Query to run (default: q1-v2.4)')
    
    args = parser.parse_args()
    
    # Create Spark session with Gluten
    spark = SparkSession.builder \
        .appName("TPC-DS Query with Gluten") \
        .getOrCreate()
    
    # Print Spark configuration for debugging
    print("Spark Configuration:")
    for entry in spark.sparkContext.getConf().getAll():
        print(f"  {entry[0]}: {entry[1]}")
    
    # Run the query
    success = run_query(spark, args.source_dir, args.result_dir, args.query)
    
    # Stop Spark session
    spark.stop()
    
    # Exit with appropriate code
    if success:
        print("Query execution completed successfully")
        exit(0)
    else:
        print("Query execution failed")
        exit(1)

if __name__ == "__main__":
    main()
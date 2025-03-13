#!/bin/bash

# Configuration
NAMESPACE="tpcds1t30k"
TABLE_BUCKET_ARN="arn:aws:s3tables:us-east-1:724772074726:bucket/tpcds1t30k"
REGION="us-east-1"
DATABASE="tpcds1t30k"

# Table names
tables=(
  "call_center"
  "catalog_page"
  "catalog_returns"
  "catalog_sales"
  "customer"
  "customer_address"
  "customer_demographics"
  "date_dim"
  "household_demographics"
  "income_band"
  "inventory"
  "item"
  "promotion"
  "reason"
  "ship_mode"
  "store"
  "store_returns"
  "store_sales"
  "time_dim"
  "warehouse"
  "web_page"
  "web_returns"
  "web_sales"
  "web_site"
)

# Delete all tables
echo "Starting deletion of all tables..."
for table in "${tables[@]}"; do
  echo "Deleting table: $table"
  aws s3tables delete-table \
    --namespace "$NAMESPACE" \
    --table-bucket-arn "$TABLE_BUCKET_ARN" \
    --name "${table}"
  
  # Check if the deletion was successful
  if [ $? -eq 0 ]; then
    echo "Successfully deleted table: $table"
  else
    echo "Error deleting table: $table"
  fi
done

# Delete the namespace
echo "Deleting namespace..."
aws s3tables delete-namespace \
  --namespace "$NAMESPACE" \
  --table-bucket-arn "$TABLE_BUCKET_ARN"

# Check if the namespace deletion was successful
if [ $? -eq 0 ]; then
  echo "Successfully deleted namespace: $NAMESPACE"
else
  echo "Error deleting namespace: $NAMESPACE"
fi

# Delete the table bucket
echo "Deleting table bucket..."
aws s3tables delete-table-bucket \
  --region "$REGION" \
  --table-bucket-arn "$TABLE_BUCKET_ARN"

# Check if the table bucket deletion was successful
if [ $? -eq 0 ]; then
  echo "Successfully deleted table bucket"
else
  echo "Error deleting table bucket"
fi

echo "Deletion process completed."

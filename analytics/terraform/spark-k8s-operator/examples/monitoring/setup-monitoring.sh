#!/bin/bash

# Setup monitoring for Spark jobs
set -e

NAMESPACE="spark-team-a"
PROMETHEUS_URL="http://prometheus-server:9090"
S3_BUCKET="spark-operator-doeks-spark-logs-20250409142902480900000001"

echo "Setting up Spark job monitoring..."

# Create ConfigMap with metrics collection script
kubectl create configmap metrics-collector-script -n $NAMESPACE \
  --from-file=collect-metrics.py=./collect-metrics.py \
  --dry-run=client -o yaml | kubectl apply -f -

# Apply Prometheus configuration
kubectl apply -f prometheus-config.yaml

# Function to monitor a specific Spark job
monitor_spark_job() {
    local JOB_NAME=$1
    local DURATION=${2:-60}
    
    echo "Starting monitoring for job: $JOB_NAME"
    
    # Create a job-specific metrics collector
    cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: metrics-collector-${JOB_NAME}
  namespace: $NAMESPACE
spec:
  template:
    spec:
      serviceAccountName: spark-team-a
      containers:
      - name: metrics-collector
        image: python:3.9-slim
        command: ["/bin/bash"]
        args:
          - -c
          - |
            pip install requests boto3
            # Wait for Spark job to start
            sleep 30
            # Collect metrics during job execution
            python /scripts/collect-metrics.py \
              --prometheus-url $PROMETHEUS_URL \
              --s3-bucket $S3_BUCKET \
              --job-name $JOB_NAME \
              --duration $DURATION
        volumeMounts:
        - name: scripts
          mountPath: /scripts
        env:
        - name: AWS_REGION
          value: "us-east-1"
      volumes:
      - name: scripts
        configMap:
          name: metrics-collector-script
          defaultMode: 0755
      restartPolicy: OnFailure
  backoffLimit: 3
EOF

    echo "Metrics collector job created for $JOB_NAME"
}

# Function to get metrics summary from S3
get_metrics_summary() {
    local JOB_NAME=$1
    
    echo "Fetching metrics summary for job: $JOB_NAME"
    
    # List available metrics files
    aws s3 ls s3://$S3_BUCKET/spark-metrics/$JOB_NAME/ --recursive
    
    # Get the latest summary report
    LATEST_SUMMARY=$(aws s3 ls s3://$S3_BUCKET/spark-metrics/$JOB_NAME/ --recursive | grep summary_report | sort | tail -1 | awk '{print $4}')
    
    if [ -n "$LATEST_SUMMARY" ]; then
        echo "Latest summary report: $LATEST_SUMMARY"
        aws s3 cp s3://$S3_BUCKET/$LATEST_SUMMARY ./latest_summary.json
        cat ./latest_summary.json | jq '.'
    else
        echo "No summary report found for job $JOB_NAME"
    fi
}

# Function to create a simple dashboard
create_dashboard() {
    echo "Creating Grafana dashboard..."
    
    # Apply Grafana dashboard ConfigMap
    kubectl create configmap spark-dashboard -n $NAMESPACE \
      --from-file=dashboard.json=./grafana-dashboard.json \
      --dry-run=client -o yaml | kubectl apply -f -
    
    echo "Dashboard ConfigMap created. Import it into your Grafana instance."
}

# Main execution
case "${1:-setup}" in
    "setup")
        echo "Setting up monitoring infrastructure..."
        create_dashboard
        ;;
    "monitor")
        if [ -z "$2" ]; then
            echo "Usage: $0 monitor <job-name> [duration-minutes]"
            exit 1
        fi
        monitor_spark_job "$2" "${3:-60}"
        ;;
    "summary")
        if [ -z "$2" ]; then
            echo "Usage: $0 summary <job-name>"
            exit 1
        fi
        get_metrics_summary "$2"
        ;;
    *)
        echo "Usage: $0 {setup|monitor|summary}"
        echo "  setup                    - Set up monitoring infrastructure"
        echo "  monitor <job> [duration] - Monitor a specific job"
        echo "  summary <job>            - Get metrics summary for a job"
        exit 1
        ;;
esac
#!/usr/bin/env python3

import requests
import json
import time
import os
import boto3
from datetime import datetime
import argparse

class SparkMetricsCollector:
    def __init__(self, prometheus_url, s3_bucket, job_name):
        self.prometheus_url = prometheus_url
        self.s3_bucket = s3_bucket
        self.job_name = job_name
        self.s3_client = boto3.client('s3')
        self.metrics_data = []
        
    def query_prometheus(self, query, start_time=None, end_time=None):
        """Query Prometheus for metrics data"""
        try:
            if start_time and end_time:
                # Range query
                params = {
                    'query': query,
                    'start': start_time,
                    'end': end_time,
                    'step': '15s'
                }
                response = requests.get(f"{self.prometheus_url}/api/v1/query_range", params=params)
            else:
                # Instant query
                params = {'query': query}
                response = requests.get(f"{self.prometheus_url}/api/v1/query", params=params)
            
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error querying Prometheus: {e}")
            return None
    
    def collect_spark_metrics(self, start_time, end_time):
        """Collect comprehensive Spark metrics"""
        queries = {
            'driver_cpu': 'rate(container_cpu_usage_seconds_total{pod=~".*-driver", namespace="spark-team-a"}[5m]) * 100',
            'driver_memory': 'container_memory_usage_bytes{pod=~".*-driver", namespace="spark-team-a"}',
            'executor_cpu': 'rate(container_cpu_usage_seconds_total{pod=~".*-exec.*", namespace="spark-team-a"}[5m]) * 100',
            'executor_memory': 'container_memory_usage_bytes{pod=~".*-exec.*", namespace="spark-team-a"}',
            'spark_stages': 'spark_driver_DAGScheduler_stage_runningStages',
            'spark_jobs': 'spark_driver_DAGScheduler_job_allJobs',
            'jvm_heap_driver': 'spark_driver_jvm_heap_used',
            'jvm_heap_executor': 'spark_executor_jvm_heap_used',
            'gc_time': 'spark_driver_jvm_PS_MarkSweep_time',
            'shuffle_read': 'spark_executor_shuffle_read_bytes',
            'shuffle_write': 'spark_executor_shuffle_write_bytes'
        }
        
        metrics_results = {}
        for metric_name, query in queries.items():
            print(f"Collecting {metric_name}...")
            result = self.query_prometheus(query, start_time, end_time)
            if result and result.get('status') == 'success':
                metrics_results[metric_name] = result['data']['result']
            else:
                print(f"Failed to collect {metric_name}")
                
        return metrics_results
    
    def save_to_s3(self, data, filename):
        """Save metrics data to S3"""
        try:
            key = f"spark-metrics/{self.job_name}/{filename}"
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=key,
                Body=json.dumps(data, indent=2),
                ContentType='application/json'
            )
            print(f"Metrics saved to s3://{self.s3_bucket}/{key}")
            return True
        except Exception as e:
            print(f"Error saving to S3: {e}")
            return False
    
    def generate_summary_report(self, metrics_data):
        """Generate a summary report of resource usage"""
        summary = {
            'job_name': self.job_name,
            'collection_time': datetime.now().isoformat(),
            'summary': {}
        }
        
        # Calculate averages and peaks
        for metric_name, data in metrics_data.items():
            if data:
                values = []
                for series in data:
                    for value_pair in series.get('values', []):
                        try:
                            values.append(float(value_pair[1]))
                        except (ValueError, IndexError):
                            continue
                
                if values:
                    summary['summary'][metric_name] = {
                        'avg': sum(values) / len(values),
                        'max': max(values),
                        'min': min(values),
                        'samples': len(values)
                    }
        
        return summary
    
    def collect_and_save(self, duration_minutes=60):
        """Main method to collect and save metrics"""
        end_time = int(time.time())
        start_time = end_time - (duration_minutes * 60)
        
        print(f"Collecting metrics for job {self.job_name}")
        print(f"Time range: {datetime.fromtimestamp(start_time)} to {datetime.fromtimestamp(end_time)}")
        
        # Collect metrics
        metrics_data = self.collect_spark_metrics(start_time, end_time)
        
        # Generate timestamp for files
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save raw metrics data
        raw_filename = f"raw_metrics_{timestamp}.json"
        self.save_to_s3(metrics_data, raw_filename)
        
        # Generate and save summary report
        summary = self.generate_summary_report(metrics_data)
        summary_filename = f"summary_report_{timestamp}.json"
        self.save_to_s3(summary, summary_filename)
        
        # Print summary to console
        print("\n=== RESOURCE USAGE SUMMARY ===")
        for metric, stats in summary['summary'].items():
            print(f"{metric}:")
            print(f"  Average: {stats['avg']:.2f}")
            print(f"  Maximum: {stats['max']:.2f}")
            print(f"  Minimum: {stats['min']:.2f}")
            print(f"  Samples: {stats['samples']}")
            print()

def main():
    parser = argparse.ArgumentParser(description='Collect Spark job metrics')
    parser.add_argument('--prometheus-url', required=True, help='Prometheus server URL')
    parser.add_argument('--s3-bucket', required=True, help='S3 bucket for storing metrics')
    parser.add_argument('--job-name', required=True, help='Spark job name')
    parser.add_argument('--duration', type=int, default=60, help='Duration in minutes to collect metrics')
    
    args = parser.parse_args()
    
    collector = SparkMetricsCollector(args.prometheus_url, args.s3_bucket, args.job_name)
    collector.collect_and_save(args.duration)

if __name__ == "__main__":
    main()
import argparse
import multiprocessing
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
import json
import random
from datetime import datetime, timedelta

from consts.constants import Constants

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Promik data gen") \
    .getOrCreate()


def parse_arguments():
    parser = argparse.ArgumentParser(description="Generate random timeseries data and save as Parquet files.")
    parser.add_argument('--config-name',
                        type=str,
                        default=Constants.InputConsts.DEFAULT_CONFIG_NAME,
                        help='Name of the config')

    return parser.parse_args()


# Function to generate random CPU usage
def generate_random_usage(min_val, max_val):
    return random.uniform(min_val, max_val)


# Function to generate random throttled seconds
def generate_random_throttled_seconds(is_high_utilization, interval_secs):
    if is_high_utilization:
        return random.uniform(0.20, 1.00)
    return 0


# Function to write data to Parquet
def write_to_parquet(data, schema, file):
    df = spark.createDataFrame(data, schema=schema)
    df.write.mode("append").parquet(file)


def generate_metric_data(start_time, end_time, interval_secs, config, schema, metrics, config_name, is_cpu, is_mem, is_gpu):
    timestamps = get_timestamps(from_ts=start_time, to_ts=end_time, interval_secs=interval_secs)

    metrics_data = []
    file_name = "cpu"
    if is_mem:
        file_name = "memory"
    if is_gpu:
        file_name = "gpu"
    file_to_write = f"./data/configs/{config_name}/{file_name}.parquet"
    for timestamp in timestamps:
        for cluster_name, cluster_info in config['clusters'].items():
            for namespace_name, namespace_info in cluster_info['namespaces'].items():
                for workload_name, workload_info in namespace_info['workloads'].items():
                    is_gpu_workload = workload_info['is_gpu_workload']
                    for pod_name in workload_info['pods']:
                        pod_state = workload_info['pod_states'][pod_name]
                        pod_id = pod_state['id']
                        pod_node = pod_state['node']
                        gpu_uuid = None
                        gpu_device = None
                        gpu_model = None
                        if is_gpu and is_gpu_workload:
                            gpu_uuid = pod_state['gpu_uuid']
                            gpu_device = pod_state['gpu_device']
                            gpu_model = pod_state['gpu_model']
                        for container_name in workload_info['containers_list']:
                            container_info = workload_info['containers'][container_name]
                            container_id = pod_state[container_name]['container_id']
                            container_image_id = container_info['image_id']
                            container_image = container_info['image']
                            value = 0.00
                            if is_cpu:
                                is_high_utilization = container_info['resources']['cpu']['utilization'] == "high"
                                min_cpu = container_info['resources']['cpu']['min']
                                max_cpu = container_info['resources']['cpu']['max']

                                previous_cpu_seconds = pod_state[container_name]['current_cpu_seconds']
                                previous_throttled_seconds = pod_state[container_name]['current_throttled_seconds']

                                cpu_usage = generate_random_usage(min_cpu, max_cpu)
                                throttled_seconds = generate_random_throttled_seconds(is_high_utilization, interval_secs)

                                new_cpu_seconds = previous_cpu_seconds + (cpu_usage * interval_secs)
                                new_throttled_seconds = previous_throttled_seconds + (throttled_seconds * interval_secs)

                                pod_state[container_name]['current_cpu_seconds'] = new_cpu_seconds
                                pod_state[container_name]['current_throttled_seconds'] = new_throttled_seconds

                                for metric in metrics:
                                    if metric == "container_cpu_usage_seconds_total":
                                        value = new_cpu_seconds
                                    else:
                                        value = throttled_seconds
                                    metric_entry = {
                                        "timestamp": timestamp,
                                        "value": float(value),
                                        "metric_name": metric,
                                        "container": container_name,
                                        "pod": pod_name,
                                        "endpoint": "http-metrics",
                                        "id": f"/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod{pod_id}.slice/crio-{container_id}.scope",
                                        "image": container_image,
                                        "namespace": namespace_name,
                                        "node": pod_node,
                                        "service": "kubelet"
                                    }
                                    metrics_data.append(metric_entry)
                            if is_mem:
                                min_mem = container_info['resources']['memory']['min']
                                max_mem = container_info['resources']['memory']['max']
                                for metric in metrics:
                                    memory_usage = generate_random_usage(min_mem, max_mem)
                                    metric_entry = {
                                        "timestamp": timestamp,
                                        "value": float(memory_usage),
                                        "metric_name": metric,
                                        "container": container_name,
                                        "pod": pod_name,
                                        "endpoint": "http-metrics",
                                        "id": f"/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod{pod_id}.slice/crio-{container_id}.scope",
                                        "image": container_image,
                                        "namespace": namespace_name,
                                        "node": pod_node,
                                        "service": "kubelet"
                                    }
                                    metrics_data.append(metric_entry)
                            if is_gpu and is_gpu_workload:
                                min_gpu = container_info['resources']['gpu']['min']
                                max_gpu = container_info['resources']['gpu']['max']
                                for metric in metrics:
                                    gpu_usage = generate_random_usage(min_gpu, max_gpu)
                                    metric_entry = {
                                        "timestamp": timestamp,
                                        "value": float(gpu_usage),
                                        "metric_name": metric,
                                        "DCGM_FI_DRIVER_VERSION": "550.54.15",
                                        "Hostname": pod_node,
                                        "UUID": f"GPU-{gpu_uuid}",
                                        "container": "nvidia-dcgm-exporter",
                                        "device": gpu_device,
                                        "endpoint": "gpu-metrics",
                                        "exported_container": container_name,
                                        "exported_namespace": namespace_name,
                                        "exported_pod": pod_name,
                                        "job": "nvidia-dcgm-exporter",
                                        "modelName": gpu_model,
                                        "namespace": "nvidia-gpu-operator",
                                        "pod": "nvidia-dcgm-exporter-4jvhr",
                                        "service": "nvidia-dcgm-exporter"
                                    }
                                    metrics_data.append(metric_entry)
                if len(metrics_data) >= 500000:
                    write_to_parquet(metrics_data, schema=schema, file=file_to_write)
                    metrics_data = []

    if metrics_data:
        write_to_parquet(metrics_data, schema, file=file_to_write)
    print("Done")


def get_timestamps(from_ts: int, to_ts: int, interval_secs: int):
    if from_ts <= 0 or to_ts <= 0 or interval_secs <= 0 or from_ts >= to_ts:
        return None

    timestamps = []
    current_ts = from_ts

    while current_ts <= to_ts:
        timestamps.append(current_ts)
        current_ts += interval_secs

    return timestamps

def generate_and_write_metrics(start_time, end_time, interval_secs, config, schema, metric_names, config_name, is_cpu_mem):
    metrics_data = (start_time, end_time, interval_secs, config, schema, metric_names, config_name, is_cpu_mem)


def start_processes_for_metrics(start_time, end_time, interval_secs, config, resource_map, config_name):
    base_schema = [
        StructField("timestamp", IntegerType(), True),
        StructField("value", DoubleType(), True),
        StructField("metric_name", StringType(), True),
    ]

    cpu_mem_schema = base_schema + [
        StructField("container", StringType(), True),
        StructField("endpoint", StringType(), True),
        StructField("id", StringType(), True),
        StructField("image", StringType(), True),
        StructField("job", StringType(), True),
        StructField("namespace", StringType(), True),
        StructField("node", StringType(), True),
        StructField("pod", StringType(), True),
        StructField("service", StringType(), True)
    ]

    gpu_schema = base_schema + [
        StructField("DCGM_FI_DRIVER_VERSION", StringType(), True),
        StructField("Hostname", StringType(), True),
        StructField("UUID", StringType(), True),
        StructField("container", StringType(), True),
        StructField("device", StringType(), True),
        StructField("endpoint", StringType(), True),
        StructField("modelName", StringType(), True),
        StructField("namespace", StringType(), True),
        StructField("service", StringType(), True),
        StructField("exported_container", StringType(), True),
        StructField("exported_namespace", StringType(), True),
        StructField("exported_pod", StringType(), True),
        StructField("pod", StringType(), True),
        StructField("job", StringType(), True)
    ]

    print(cpu_mem_schema)
    processes = []
    schema = None
    for key, metric_names in resource_map.items():
        is_cpu = False
        is_mem = False
        is_gpu = False
        if key == "cpu":
            is_cpu = True
            is_mem = False
            is_gpu = False
            schema = StructType(cpu_mem_schema)
        elif key == "memory":
            is_cpu = False
            is_mem = True
            is_gpu = False
            schema = StructType(cpu_mem_schema)
        else:
            is_cpu = False
            is_mem = False
            is_gpu = True
            schema = StructType(gpu_schema)
        generate_metric_data(start_time, end_time, interval_secs, config, schema, metric_names, config_name, is_cpu, is_mem, is_gpu)
    #     process = multiprocessing.Process(target=generate_metric_data,
    #                                       args=(start_time, end_time, interval_secs, config, schema, metric_names, config_name, is_cpu, is_mem, is_gpu))
    #     processes.append(process)
    #     process.start()
    #
    # for process in processes:
    #     process.join()


def main():
    args = parse_arguments()
    config_name = args.config_name
    if config_name is None:
        print("Config name needed")
        exit(1)

    config_name = str(config_name).strip()

    if config_name == "":
        print("config name cannot be empty")
        exit(1)

    config_path = f"./data/configs/{config_name}/meta.json"

    if not os.path.exists(config_path):
        print("Config doesn't exist please run generate_config.py")
        exit(1)
    with open(config_path, 'r') as f:
        config = json.load(f)

    start_time = config['time_range']['start_time']
    end_time = config['time_range']['end_time']
    interval_secs = config['time_range']['interval_secs']

    resource_map = {
        "cpu": [
            "container_cpu_usage_seconds_total",
            "container_cpu_cfs_throttled_seconds_total"
        ],
        "memory": [
            "container_memory_usage_bytes",
            # "container_memory_rss"
        ],
        "gpu": [
            "DCGM_FI_DEV_GPU_UTIL",
            "DCGM_FI_DEV_MEM_COPY_UTIL"
        ]
    }
    start_processes_for_metrics(start_time=start_time,
                                end_time=end_time,
                                interval_secs=interval_secs,
                                config=config,
                                resource_map=resource_map,
                                config_name=config_name)

    spark.stop()


if __name__ == "__main__":
    main()

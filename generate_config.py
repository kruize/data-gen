#############################################################################
#    Copyright (c) 2026 Red Hat, IBM Corporation and others.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#############################################################################
import argparse
import hashlib
import json
import os
import random
import re
import string
import time
import uuid
from datetime import datetime, timedelta, UTC

from consts.constants import Constants

CONFIG_LOCATION = "./data/configs"
MAPPING_JSON = "./data/metadata/mapping.json"
META_JSON_FILE_NAME = "meta.json"

NUM_NODES = 50000
CPU_CHOICES = [8, 16, 32]
MEM_CHOICES = [32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024]
GPU_CHOICES = ['A100', 'H100', None]
GPU_COUNT_CHOICES = [2, 4, 8]

NODE_MAP = {}

for node_index in range(NUM_NODES):
    gpu_type = random.choice(GPU_CHOICES)
    worker_name = f"ip-10-0-{random.randint(0, 255)}-{random.randint(0, 255)}"
    gpu_info = None
    if gpu_type is not None:
        worker_name = f"{worker_name}-gpu-worker-ec2-internal"
        gpu_info = {
            "type": gpu_type,
            "count": random.choice(GPU_COUNT_CHOICES)
        }
    else:
        worker_name = f"{worker_name}-worker-ec2-internal"
    init_cpu = random.choice(CPU_CHOICES)
    init_mem = random.choice(MEM_CHOICES)
    NODE_MAP[worker_name] = {
        "is_assigned": False,
        "capacity": {
            "cpu": init_cpu,
            "memory": init_mem,
            "gpu": gpu_info
        },
        "availability": {
            "cpu": init_cpu,
            "memory": init_mem,
            "gpu": gpu_info.copy() if gpu_info else None
        }
    }


def get_node(is_gpu_workload, max_cpu, max_memory):
    for node_name, node_info in NODE_MAP.items():
        if not node_info["is_assigned"]:
            if (node_info["availability"]["cpu"] >= max_cpu and
                    node_info["availability"]["memory"] >= max_memory):
                if is_gpu_workload:
                    if node_info["availability"]["gpu"] and node_info["availability"]["gpu"]["count"] > 0:
                        node_info["availability"]["cpu"] -= max_cpu
                        node_info["availability"]["memory"] -= max_memory
                        node_info["availability"]["gpu"]["count"] -= 1
                        node_info["is_assigned"] = True
                        return node_name, node_info["availability"]["gpu"]["type"]
                else:
                    node_info["availability"]["cpu"] -= max_cpu
                    node_info["availability"]["memory"] -= max_memory
                    node_info["is_assigned"] = True
                    return node_name, None
    return None, None



def parse_arguments():
    parser = argparse.ArgumentParser(description="Generate config for workloads")
    parser.add_argument('--config-name',
                        type=str,
                        default=Constants.InputConsts.DEFAULT_CONFIG_NAME,
                        help='Name of the config')
    parser.add_argument('--num-namespaces',
                        type=int,
                        default=Constants.InputConsts.DEFAULT_NUM_NAMESPACES,
                        help='Number of namespaces')
    parser.add_argument('--min-deployments',
                        type=int,
                        default=Constants.InputConsts.DEFAULT_MIN_DEPLOYMENTS,
                        help='Minimum number of deployments per namespace')
    parser.add_argument('--max-deployments',
                        type=int,
                        default=Constants.InputConsts.DEFAULT_MAX_DEPLOYMENTS,
                        help='Maximum number of deployments per namespace')
    parser.add_argument('--min-replicas',
                        type=int,
                        default=Constants.InputConsts.DEFAULT_MIN_REPLICAS,
                        help='Minimum number of replicas per deployment')
    parser.add_argument('--max-replicas',
                        type=int,
                        default=Constants.InputConsts.DEFAULT_MAX_REPLICAS,
                        help='Maximum number of replicas per deployment')
    parser.add_argument("--pre-days",
                        type=int,
                        default=Constants.InputConsts.DEFAULT_PRE_DAYS,
                        help='Number of days the data need to be generated before now')
    parser.add_argument("--post-days",
                        type=int,
                        default=Constants.InputConsts.DEFAULT_POST_DAYS,
                        help='Number of days the data need to be generated after now')
    parser.add_argument("--interval",
                        default=Constants.InputConsts.DEFAULT_INTERVAL,
                        choices=Constants.INTERVAL_CHOICES, help="Time interval between entries")
    return parser.parse_args()


def get_num_secs(secs: str):
    if secs is None:
        return 30
    if secs == "":
        return 30
    secs = secs.strip()
    if secs not in Constants.INTERVAL_CHOICES:
        return 30
    if secs == "1s":
        return 1
    if secs == "5s":
        return 5
    if secs == "15s":
        return 15
    if secs == "30s":
        return 30
    if secs == "60s":
        return 60


def get_start_end_unix_seconds(pre_days: int, post_days: int):
    current_time = datetime.now(UTC).replace(second=0, microsecond=0)
    start_time = current_time - timedelta(days=pre_days)
    end_time = current_time + timedelta(days=post_days)
    start_unix_seconds = int(time.mktime(start_time.timetuple()))
    end_unix_seconds = int(time.mktime(end_time.timetuple()))
    return start_unix_seconds, end_unix_seconds


def get_timestamps(from_ts: int, to_ts: int, interval_secs: int):
    if from_ts <= 0 or to_ts <= 0 or interval_secs <= 0 or from_ts >= to_ts:
        return None

    timestamps = []
    current_ts = from_ts

    while current_ts <= to_ts:
        timestamps.append(current_ts)
        current_ts += interval_secs

    return timestamps


def create_config_dir(config_name: str):
    return_path = os.path.join(CONFIG_LOCATION, config_name)
    if not os.path.exists(return_path):
        os.makedirs(return_path)
    else:
        print(f"Config with name '{config_name}' already exists. Content will be overridden")
    return return_path


def load_metadata():
    try:
        with open(MAPPING_JSON, 'r') as f:
            metadata = json.load(f)
        return metadata
    except FileNotFoundError:
        print(f"Error: File '{MAPPING_JSON}' not found.")
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from '{MAPPING_JSON}': {str(e)}")
        return None


def generate_pod_name(deployment_name: str, is_deployment: bool, replica_index: int = None):
    if is_deployment:
        random_suffix = ''.join(random.choices(string.ascii_lowercase, k=6))
        return f"{deployment_name}-pod-{random_suffix}"
    else:
        return f"{deployment_name}-{replica_index}"


def generate_container_name(deployment_name: str):
    names = Constants.KRUIZE_TEAM_NAMES
    return f"{deployment_name}-{random.choice(names)}"





def generate_meta_json(
        config_path: str,
        num_namespaces: int,
        min_deployments: int,
        max_deployments: int,
        min_replicas: int,
        max_replicas: int,
        start_time: int,
        end_time: int,
        interval_secs: int,
        mapping_data: dict
):
    config_file = f"{config_path}/{META_JSON_FILE_NAME}"

    meta = {"time_range": {
        "start_time": start_time,
        "end_time": end_time,
        "interval_secs": interval_secs
    }}
    selected_clusters = []
    all_namespaces = [country for continent in mapping_data.values() for country in continent.keys()]
    selected_namespaces = random.sample(all_namespaces, min(num_namespaces, len(all_namespaces)))
    for continent, countries in mapping_data.items():
        if any(country in countries for country in selected_namespaces):
            selected_clusters.append(continent)

    meta['cluster_list'] = selected_clusters
    meta['num_namespaces'] = len(selected_namespaces)
    meta['num_clusters'] = len(selected_clusters)
    meta['clusters'] = {}
    cluster_dict = meta['clusters']
    for cluster in selected_clusters:
        namespaces_in_cluster = list(dict(mapping_data[cluster]).keys())
        selected_namespaces_in_cluster = [ns for ns in namespaces_in_cluster if ns in selected_namespaces]
        cluster_dict[cluster] = {
            "namespaces_list": selected_namespaces_in_cluster,
            "num_namespaces": len(selected_namespaces_in_cluster),
            "namespaces": {}
        }
        namespaces_main_dict = cluster_dict[cluster]['namespaces']
        for namespace in selected_namespaces_in_cluster:
            namespaces_main_dict[namespace] = {}
            namespaces_dict = namespaces_main_dict[namespace]
            all_workloads = mapping_data[cluster][namespace]
            num_workloads = random.randint(min_deployments, max_deployments)
            selected_workloads = random.sample(all_workloads, min(num_workloads, len(all_workloads)))
            namespaces_dict['workloads_list'] = selected_workloads
            namespaces_dict['num_workloads'] = len(selected_workloads)
            namespaces_dict['workloads'] = {}
            workload_dict = namespaces_dict['workloads']
            for workload in selected_workloads:
                cpu_utilization = random.choice(["idle", "low", "medium", "high"])
                memory_utilization = random.choice(["low", "medium", "high"])
                gpu_utilization = random.choice(["low", "medium", "high"])


                is_deployment = random.choice([True, False])
                replicas = random.randint(min_replicas, max_replicas)
                is_gpu_workload = False
                if is_deployment:
                    is_gpu_workload = random.choice([True, False])

                num_containers = random.choice([1, 2])
                containers_dict = {}
                container_list = []

                min_cpu = Constants.ResourceConstraints.MIN_CPU
                max_cpu = Constants.ResourceConstraints.MAX_CPU
                min_memory = Constants.ResourceConstraints.MIN_MEMORY
                max_memory = Constants.ResourceConstraints.MAX_MEMORY
                min_gpu = 0
                max_gpu = 0
                if is_gpu_workload:
                    min_gpu = Constants.ResourceConstraints.MIN_GPU
                    max_gpu = Constants.ResourceConstraints.MAX_GPU

                if cpu_utilization == "idle":
                    min_cpu = Constants.ResourceConstraints.IDLE_CPU_MIN
                    max_cpu = Constants.ResourceConstraints.IDLE_CPU_MAX
                elif cpu_utilization == "low":
                    min_cpu = Constants.ResourceConstraints.MIN_MIN_CPU
                    max_cpu = Constants.ResourceConstraints.MAX_MIN_CPU
                elif cpu_utilization == "medium":
                    min_cpu = Constants.ResourceConstraints.MIN_AVG_CPU
                    max_cpu = Constants.ResourceConstraints.MAX_AVG_CPU
                else:
                    min_cpu = Constants.ResourceConstraints.MIN_MAX_CPU
                    max_cpu = Constants.ResourceConstraints.MAX_MAX_CPU

                if memory_utilization == "low":
                    min_memory = Constants.ResourceConstraints.MIN_MIN_MEMORY
                    max_memory = Constants.ResourceConstraints.MAX_MIN_MEMORY
                elif memory_utilization == "medium":
                    min_memory = Constants.ResourceConstraints.MIN_AVG_MEMORY
                    max_memory = Constants.ResourceConstraints.MAX_AVG_MEMORY
                else:
                    min_memory = Constants.ResourceConstraints.MIN_MAX_MEMORY
                    max_memory = Constants.ResourceConstraints.MAX_MAX_MEMORY

                if is_gpu_workload:
                    if gpu_utilization == "low":
                        min_gpu = Constants.ResourceConstraints.MIN_MIN_GPU
                        max_gpu = Constants.ResourceConstraints.MAX_MIN_GPU
                    elif gpu_utilization == "medium":
                        min_gpu = Constants.ResourceConstraints.MIN_AVG_GPU
                        max_gpu = Constants.ResourceConstraints.MAX_AVG_GPU
                    else:
                        min_gpu = Constants.ResourceConstraints.MIN_MAX_GPU
                        max_gpu = Constants.ResourceConstraints.MAX_MAX_GPU

                for i in range(0, num_containers):
                    container_name = generate_container_name(deployment_name=workload)
                    container_list.append(container_name)

                pods = []
                pod_states = {}
                for i in range(0, replicas):
                    pod = generate_pod_name(deployment_name=workload, is_deployment=is_deployment, replica_index=i)
                    pod_id = str(uuid.uuid4())
                    pods.append(pod)
                    node_name, available_gpu_type = get_node(is_gpu_workload, max_cpu, max_memory)
                    pod_states[pod] = {
                        "id": pod_id,
                        "node": node_name
                    }
                    if is_gpu_workload:
                        pod_states[pod]['gpu_uuid'] = str(uuid.uuid4())
                        pod_states[pod]['gpu_device'] = "nvidia0"
                        pod_states[pod]['gpu_model'] = available_gpu_type
                    for container_name in container_list:
                        pod_states[pod][container_name] = {
                            "container_id": hashlib.sha256(os.urandom(64)).hexdigest(),
                            "current_cpu_seconds": 0,
                            "current_throttled_seconds": 0
                        }

                for container_name in container_list:
                    containers_dict[container_name] = {
                        "image_id": hashlib.sha256(os.urandom(64)).hexdigest(),
                        "image": f"quay.io/abmc/{container_name}:latest",
                        "resources": {
                            "cpu": {
                                "utilization": cpu_utilization,
                                "min": min_cpu,
                                "max": max_cpu
                            },
                            "memory": {
                                "utilization": memory_utilization,
                                "min": min_memory,
                                "max": max_memory
                            }
                        }
                    }
                    if is_gpu_workload:
                        containers_dict[container_name]['resources']['gpu'] = {
                            "utilization": gpu_utilization,
                            "min": min_gpu,
                            "max": max_gpu
                        }

                workload_dict[workload] = {
                    "num_replicas": replicas,
                    "is_deployment": is_deployment,
                    "is_gpu_workload": is_gpu_workload,
                    "pods": pods,
                    "pod_states": pod_states,
                    "num_containers": num_containers,
                    "containers_list": container_list,
                    "containers": containers_dict
                }
    with open(config_file, "w") as json_file:
        json.dump(meta, json_file, ensure_ascii=False, indent=4)


def main():
    args = parse_arguments()
    num_namespaces = args.num_namespaces
    min_deployments = args.min_deployments
    max_deployments = args.max_deployments
    min_replicas = args.min_replicas
    max_replicas = args.max_replicas
    pre_days = args.pre_days
    post_days = args.post_days
    interval = args.interval
    interval_secs = get_num_secs(interval)
    config_name = args.config_name

    if not re.match(r"^[a-zA-Z0-9_-]+$", config_name):
        raise ValueError(
            "Invalid characters in config_name. Only alphanumeric, hyphen (-), and underscore (_) are allowed.")

    if '/' in config_name:
        raise ValueError("config_name cannot contain '/' character.")

    if pre_days <= 0:
        pre_days = Constants.InputConsts.DEFAULT_PRE_DAYS

    if post_days < 0:
        post_days = Constants.InputConsts.DEFAULT_POST_DAYS

    start_unix_seconds, end_unix_seconds = get_start_end_unix_seconds(pre_days, post_days)
    timestamps = get_timestamps(from_ts=start_unix_seconds,
                                to_ts=end_unix_seconds,
                                interval_secs=interval_secs)

    if timestamps is not None:
        print(len(timestamps))

    config_dir = create_config_dir(config_name=config_name)
    metadata = load_metadata()
    generate_meta_json(config_path=config_dir,
                       num_namespaces=num_namespaces,
                       min_deployments=min_deployments,
                       max_deployments=max_deployments,
                       min_replicas=min_replicas,
                       max_replicas=max_replicas,
                       start_time=start_unix_seconds,
                       end_time=end_unix_seconds,
                       interval_secs=interval_secs,
                       mapping_data=metadata)


if __name__ == "__main__":
    main()
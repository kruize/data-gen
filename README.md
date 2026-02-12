# data-gen

# Synthetic Kubernetes Workload Usage Generator

This project generates **synthetic Kubernetes-style metrics** and converts it into **TSDB blocks**.
The goal is to create realistic, large-scale time-series data for testing ingestion pipelines, storage systems, and query performance without requiring real cluster metrics.

The pipeline simulates infrastructure, produces time-series signals, converts them into OpenMetrics format, and finally builds TSDB blocks.

---

## Data Generation Flow

The pipeline runs in four stages:

### 1. Configuration generation

A synthetic environment configuration is created first.

* Defines clusters, namespaces, workloads, pods, and containers
* Assigns resource limits and utilization behavior
* Schedules workloads onto simulated nodes
* Defines the time range and sampling interval

The configuration is generated **from `mapping.json`**, which provides the base cluster → namespace → workload relationships.
The output is a `meta.json` file that fully describes the simulated environment.

---

### 2. Synthetic metric generation

Using the generated configuration:

* Time-series metrics are emitted for CPU, memory, and GPU
* Metrics are generated per container across the full time window
* Cumulative counters evolve over time
* Labels reflect infrastructure topology and runtime identity

This produces structured metric data aligned with the environment model.

---

### 3. OpenMetrics conversion

Generated metric data is converted into **OpenMetrics (OMF)** format using a converter script.

This prepares the data for ingestion into Prometheus-compatible tooling.

---

### 4. TSDB block creation

OpenMetrics data is converted into **TSDB blocks** using the Prometheus `promtool` utility.

These blocks can be loaded directly into a TSDB for benchmarking or testing.

---

## Project Structure

```
.
├── generate_config.py              # Creates synthetic environment config
├── data-gen.py                     # Generates time-series metrics
├── consts/                         # Resource limits and constants
├── data/
│   ├── metadata/
│   │   └── mapping.json            # Base topology mapping
│   └── configs/
│       └── <config_name>/
│           └── meta.json
│
├── converters/                     # Metric → OpenMetrics conversion (external)
├── tsdb/                           # OMF → TSDB block creation (promtool)
```

---

## How to Run

### Step 1 — Generate configuration

```
python generate_config.py --config-name <config_name>
```

This creates:

```
data/configs/<config_name>/meta.json
```

The configuration defines the entire simulated environment.

---

### Step 2 — Generate synthetic metrics

```
python data-gen.py --config-name <config_name>
```

Metrics are generated using the configuration.

---

### Step 3 — Convert metrics to OpenMetrics

Yet to be implemented

---

### Step 4 — Generate TSDB blocks

Yet to be implemented



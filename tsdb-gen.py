import argparse
import json
import os
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor, lit

WINDOW = 12 * 60 * 60


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--config-name", required=True)
    return p.parse_args()


def build_label_string(row, label_cols):
    parts = []
    for c in label_cols:
        v = row[c]
        if v is None:
            continue
        parts.append(f'{c}="{str(v)}"')
    return "{" + ",".join(parts) + "}" if parts else ""


def write_omf(df, out_path, label_cols):
    sort_cols = ["metric"] + label_cols + ["timestamp"]
    rows = df.sort(*sort_cols).collect()

    with open(out_path, "w") as f:
        for r in rows:
            metric = r["metric"]
            ts = r["timestamp"]
            val = r["value"]
            labels = build_label_string(r, label_cols)
            f.write(f"{metric}{labels} {val} {ts}\n")

        f.write("# EOF\n")



def run_promtool(omf_file, tsdb_dir):
    cmd = [
        "docker", "run", "--rm",
        "--user", f"{os.getuid()}:{os.getgid()}",
        "--entrypoint", "promtool",
        "-v", f"{os.path.abspath(omf_file)}:/input.omf:Z",
        "-v", f"{os.path.abspath(tsdb_dir)}:/tsdb:Z",
        "prom/prometheus:latest",
        "tsdb", "create-blocks-from", "openmetrics",
        "/input.omf",
        "/tsdb"
    ]
    subprocess.run(cmd, check=True)


def main():
    args = parse_args()

    config_name = args.config_name.strip()
    base = f"./data/configs/{config_name}"
    meta_file = f"{base}/meta.json"

    if not os.path.exists(meta_file):
        raise RuntimeError("Config not found")

    with open(meta_file) as f:
        config = json.load(f)

    start = config["time_range"]["start_time"]

    spark = SparkSession.builder.appName("OMF generator").getOrCreate()

    # load all parquet files
    dfs = []
    for r in ["cpu", "memory", "gpu"]:
        p = f"{base}/{r}.parquet"
        if os.path.exists(p):
            dfs.append(spark.read.parquet(p))

    if not dfs:
        raise RuntimeError("No parquet found")

    df = dfs[0]
    for d in dfs[1:]:
        df = df.unionByName(d, allowMissingColumns=True)

    df = df.withColumnRenamed("metric_name", "metric")

    df = df.withColumn(
        "window",
        floor((col("timestamp") - lit(start)) / lit(WINDOW))
    )

    required = {"metric", "timestamp", "value", "window"}
    label_cols = [c for c in df.columns if c not in required]

    windows = [r.window for r in df.select("window").distinct().collect()]

    tsdb_dir = f"./data/tsdb/{config_name}"
    os.makedirs(tsdb_dir, exist_ok=True)

    for wid in sorted(windows):
        print(f"Processing window {wid}")
        window_df = df.filter(col("window") == wid).drop("window")
        omf_file = f"./tmp_{config_name}_{wid}.omf"
        write_omf(window_df, omf_file, label_cols)
        run_promtool(omf_file, tsdb_dir)
        os.remove(omf_file)

        print("done window", wid)

    spark.stop()
    print("All blocks created.")


if __name__ == "__main__":
    main()

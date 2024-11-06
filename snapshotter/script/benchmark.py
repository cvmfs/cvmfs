#!/usr/bin/env python3

import subprocess
import re
import json
import sys
import logging
import statistics
import os
import requests
from requests.auth import HTTPBasicAuth

from datetime import datetime


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)


def run_benchmark(iteration, image, snapshotter, task):
    logging.info(
        f"Benchmark run #{iteration} - snapshotter: {snapshotter}, image: {image}, task: {task}"
    )

    result = subprocess.run(
        f"""
            echo benchmark_start: $(date +%s%N); \
            echo pull_start: $(date +%s%N); \
            sudo nerdctl pull --snapshotter={snapshotter} {image}; \
            echo pull_end: $(date +%s%N); \
            echo run_start: $(date +%s%N); \
            sudo nerdctl run --snapshotter={snapshotter} {image} /bin/bash -c "\
            echo container_start: \$(date +%s%N); \
            {task}; \
            echo container_end: \$(date +%s%N)"; \
            echo run_end: $(date +%s%N)
        """,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    logging.info(f"Run stdout: {result.stdout}")
    logging.info(f"Run stderr: {result.stderr}")

    output = result.stdout

    benchmark_start_match = re.search(r"benchmark_start: ([\d]+)", output)
    pull_start_match = re.search(r"pull_start: ([\d]+)", output)
    pull_end_match = re.search(r"pull_end: ([\d]+)", output)
    container_start_match = re.search(r"container_start: ([\d]+)", output)
    container_end_match = re.search(r"container_end: ([\d]+)", output)
    run_end_match = re.search(r"run_end: ([\d]+)", output)

    if not all(
        [
            benchmark_start_match,
            pull_start_match,
            pull_end_match,
            container_start_match,
            container_end_match,
            run_end_match,
        ]
    ):
        sys.exit("Error: One or more timestamps not found in the output.")

    benchmark_start = int(benchmark_start_match.group(1))
    pull_start = int(pull_start_match.group(1))
    pull_end = int(pull_end_match.group(1))
    container_start = int(container_start_match.group(1))
    container_end = int(container_end_match.group(1))
    run_end = int(run_end_match.group(1))

    pull_time = (pull_end - pull_start) / 1e9
    creation_time = (container_start - pull_end) / 1e9
    execution_time = (container_end - container_start) / 1e9
    total_time = (run_end - benchmark_start) / 1e9

    return pull_time, creation_time, execution_time, total_time


def cleanup(image):
    subprocess.run(["bash", "flush_cache.sh", image], check=True)

def parse_commit_date(commit_timestamp):
    date_formats = [
        "%Y-%m-%dT%H:%M:%S%z",  
        "%Y-%m-%d %H:%M:%S %z", 
    ]

    for fmt in date_formats:
        try:
            commit_date = datetime.strptime(commit_timestamp, fmt)
            return commit_date.isoformat()
        except ValueError:
            continue
    
    raise ValueError(f"Time data '{commit_timestamp}' does not match any known format.")



def upload_benchmarks(benchmark_results, server_url, username, password):
    logging.info(f"Uploading benchmark results to {server_url}")
    response = requests.post(
        f"{server_url}/snapshotter-benchmark",
        json=benchmark_results,
        auth=HTTPBasicAuth(username, password),
    )

    if response.status_code == 200:
        logging.info(
            f"Upload completed successfully. Results at: {server_url}/snapshotter-benchmark"
        )
    else:
        logging.error(
            f"Upload failed with status code {response.status_code}: {response.text}"
        )


if __name__ == "__main__":
    data = [
        {
            "image": "docker.io/rootproject/root:6.32.02-ubuntu24.04",
            "tasks": [
                "/bin/bash",
                r"python -c 'print(\"done\")'",
                r"python -c 'import ROOT; print(\"done\")'",
                "python /opt/root/tutorials/pyroot/fillrandom.py",
            ],
        },
        {
            "image": "docker.io/library/python:3.9",
            "tasks": [
                "/bin/bash",
                r"python -c 'print(\"done\")'",
            ],
        },
    ]

    snapshotter = "cvmfs-snapshotter"
    num_runs = 5
    username = os.getenv("CVMFS_BENCHMARKS_USERNAME")
    password = os.getenv("CVMFS_BENCHMARKS_PASSWORD")
    server_url = os.getenv("CVMFS_BENCHMARKS_SERVER_URL")
    job_url = os.getenv("JOB_URL")
    commit_hash = os.getenv("COMMIT_HASH")
    commit_timestamp = os.getenv("COMMIT_TIMESTAMP")
    commit_date = parse_commit_date(commit_timestamp)

    results = []
    for entry in data:
        image, tasks = entry["image"], entry["tasks"]
        for task in tasks:
            pull_times = []
            creation_times = []
            execution_times = []
            total_times = []

            for i in range(num_runs):
                pull_time, creation_time, execution_time, total_time = run_benchmark(
                    i + 1, image, snapshotter, task
                )
                pull_times.append(pull_time)
                creation_times.append(creation_time)
                execution_times.append(execution_time)
                total_times.append(total_time)
                cleanup(image)

            results.append(
                {
                    "image": image,
                    "snapshotter": snapshotter,
                    "task": task,
                    "pull_time_mean": statistics.mean(pull_times),
                    "pull_time_median": statistics.median(pull_times),
                    "pull_time_stddev": statistics.stdev(pull_times),
                    "creation_time_mean": statistics.mean(creation_times),
                    "creation_time_median": statistics.median(creation_times),
                    "creation_time_stddev": statistics.stdev(creation_times),
                    "execution_time_mean": statistics.mean(execution_times),
                    "execution_time_median": statistics.median(execution_times),
                    "execution_time_stddev": statistics.stdev(execution_times),
                    "total_time_mean": statistics.mean(total_times),
                    "total_time_median": statistics.median(total_times),
                    "total_time_stddev": statistics.stdev(total_times),
                    "job_url": job_url,
                    "commit_hash": commit_hash,
                    "commit_date": commit_date,
                }
            )

    logging.info("Benchmark results:")
    logging.info(json.dumps(results, indent=4))

    upload_benchmarks(results, server_url, username, password)

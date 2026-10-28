"""
WSL2 Process Runner for Triggering Real Hadoop MapReduce Jobs
"""

import subprocess
import os
from typing import Generator

PROJECT_WSL_DIR = "/mnt/c/Users/veetrag/Desktop/B.Tech_IT/BDAL/Urban_Civic_Complaint_Analytics"
PIPELINE_SCRIPT = f"{PROJECT_WSL_DIR}/scripts/run_hadoop_pipeline.sh"


def run_hadoop_mapreduce_stream() -> Generator[str, None, int]:
    """
    Executes the real Hadoop MapReduce bash pipeline inside Ubuntu-22.04 WSL2
    and yields console output line-by-line in real time.
    """
    cmd = ["wsl.exe", "-d", "Ubuntu-22.04", "--", "bash", PIPELINE_SCRIPT]
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True
    )

    if process.stdout is not None:
        for line in process.stdout:
            yield line

    process.wait()
    return process.returncode

import subprocess
import os
from pathlib import Path

MAX_CHUNK_SIZE = 500 * 1024**3  # 500 GB
LOCAL_TMP_DIR = "/tmp/hdfs_transfer_chunk"
UAT_TMP_DIR = "/tmp/hdfs_transfer_chunk"
UAT_HOST = "uat-edge.example.com"
UAT_USER = "uatuser"

def run_cmd(cmd):
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        raise Exception(f"Command failed: {cmd}")
    return result.stdout.strip()

def list_hdfs_files_with_size(hdfs_path):
    output = run_cmd(f"hdfs dfs -du -s -h {hdfs_path}")
    output = run_cmd(f"hdfs dfs -du {hdfs_path}")
    files = []
    for line in output.splitlines():
        parts = line.split()
        if len(parts) >= 3:
            size_bytes = int(parts[0])
            file_path = parts[2]
            files.append((file_path, size_bytes))
    return files

def chunk_files(files):
    chunks = []
    current_chunk = []
    current_size = 0
    for fpath, fsize in files:
        if current_size + fsize > MAX_CHUNK_SIZE and current_chunk:
            chunks.append(current_chunk)
            current_chunk = []
            current_size = 0
        current_chunk.append((fpath, fsize))
        current_size += fsize
    if current_chunk:
        chunks.append(current_chunk)
    return chunks

def ensure_local_dir():
    os.makedirs(LOCAL_TMP_DIR, exist_ok=True)

def clean_local_dir():
    run_cmd(f"rm -rf {LOCAL_TMP_DIR}/*")

def copy_chunk_to_local(chunk):
    for fpath, _ in chunk:
        run_cmd(f"hdfs dfs -copyToLocal {fpath} {LOCAL_TMP_DIR}/")

def scp_chunk_to_uat():
    run_cmd(f"scp -r {LOCAL_TMP_DIR}/* {UAT_USER}@{UAT_HOST}:{UAT_TMP_DIR}/")

def remote_uat_copy_to_hdfs(uat_hdfs_target_path):
    remote_cmd = f"hdfs dfs -mkdir -p {uat_hdfs_target_path} && hdfs dfs -copyFromLocal {UAT_TMP_DIR}/* {uat_hdfs_target_path}/"
    run_cmd(f"ssh {UAT_USER}@{UAT_HOST} '{remote_cmd}'")

def clean_remote_uat_tmp():
    run_cmd(f"ssh {UAT_USER}@{UAT_HOST} 'rm -rf {UAT_TMP_DIR}/*'")

def process_transfer(from_path, to_path):
    print(f"\nTransferring from: {from_path} to: {to_path}")
    files = list_hdfs_files_with_size(from_path)
    chunks = chunk_files(files)
    print(f"Chunks to process: {len(chunks)}")

    for idx, chunk in enumerate(chunks):
        print(f"Processing chunk {idx + 1}/{len(chunks)}")
        ensure_local_dir()
        clean_local_dir()

        copy_chunk_to_local(chunk)
        scp_chunk_to_uat()
        remote_uat_copy_to_hdfs(to_path)
        clean_remote_uat_tmp()
        clean_local_dir()

def main(param_file):
    with open(param_file, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                from_path, to_path = line.split()
                process_transfer(from_path, to_path)
            except ValueError:
                print(f"Invalid line format in param file at line {line_num}: {line}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python hdfs_prod_to_uat_transfer.py <param_file>")
        sys.exit(1)
    main(sys.argv[1])
    
#python hdfs_prod_to_uat_transfer.py paths.txt

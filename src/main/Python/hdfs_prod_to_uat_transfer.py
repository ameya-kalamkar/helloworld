import subprocess
import os
import smtplib
from pathlib import Path
from datetime import datetime
from email.mime.text import MIMEText

# === Config ===
MAX_CHUNK_SIZE = 500 * 1024**3  # 500 GB
LOCAL_TMP_DIR = "/tmp/hdfs_transfer_chunk"
UAT_TMP_DIR = "/tmp/hdfs_transfer_chunk"
UAT_HOST = "uat-edge.example.com"
UAT_USER = "uatuser"

LOG_FILE = f"/tmp/hdfs_transfer_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

EMAIL_CONFIG = {
    "enabled": True,
    "smtp_server": "smtp.example.com",
    "smtp_port": 587,
    "username": "sender@example.com",
    "password": "yourpassword",
    "from_email": "sender@example.com",
    "to_email": ["recipient@example.com"]
}

# === Logging ===
def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_msg = f"[{timestamp}] {msg}"
    print(full_msg)
    with open(LOG_FILE, "a") as f:
        f.write(full_msg + "\n")

# === Email ===
def send_email(subject, body):
    if not EMAIL_CONFIG["enabled"]:
        return
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = EMAIL_CONFIG["from_email"]
        msg["To"] = ", ".join(EMAIL_CONFIG["to_email"])

        with smtplib.SMTP(EMAIL_CONFIG["smtp_server"], EMAIL_CONFIG["smtp_port"]) as server:
            server.starttls()
            server.login(EMAIL_CONFIG["username"], EMAIL_CONFIG["password"])
            server.sendmail(EMAIL_CONFIG["from_email"], EMAIL_CONFIG["to_email"], msg.as_string())
    except Exception as e:
        log(f"Failed to send email: {e}")

# === Shell Helper ===
def run_cmd(cmd):
    log(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        log(f"Error: {result.stderr.strip()}")
        raise Exception(f"Command failed: {cmd}")
    return result.stdout.strip()

# === HDFS File Listing ===
def list_hdfs_files_with_size(hdfs_path):
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

# === Transfer Steps ===
def ensure_local_dir():
    os.makedirs(LOCAL_TMP_DIR, exist_ok=True)

def clean_local_dir():
    run_cmd(f"rm -rf {LOCAL_TMP_DIR}/*")

def copy_chunk_to_local(chunk):
    for fpath, _ in chunk:
        run_cmd(f"hdfs dfs -copyToLocal {fpath} {LOCAL_TMP_DIR}/")

def scp_chunk_to_uat():
    run_cmd(f"scp -o StrictHostKeyChecking=no -r {LOCAL_TMP_DIR}/* {UAT_USER}@{UAT_HOST}:{UAT_TMP_DIR}/")

def remote_uat_copy_to_hdfs(uat_hdfs_target_path):
    remote_cmd = f"hdfs dfs -mkdir -p {uat_hdfs_target_path} && hdfs dfs -copyFromLocal {UAT_TMP_DIR}/* {uat_hdfs_target_path}/"
    run_cmd(f"ssh -o StrictHostKeyChecking=no {UAT_USER}@{UAT_HOST} '{remote_cmd}'")

def clean_remote_uat_tmp():
    run_cmd(f"ssh -o StrictHostKeyChecking=no {UAT_USER}@{UAT_HOST} 'rm -rf {UAT_TMP_DIR}/*'")

# === Core Processing ===
def process_transfer(from_path, to_path):
    try:
        log(f"\n--- Transferring from {from_path} to {to_path} ---")
        files = list_hdfs_files_with_size(from_path)
        chunks = chunk_files(files)
        log(f"Chunks to process: {len(chunks)}")

        for idx, chunk in enumerate(chunks):
            log(f"Processing chunk {idx + 1}/{len(chunks)}")
            ensure_local_dir()
            clean_local_dir()

            copy_chunk_to_local(chunk)
            scp_chunk_to_uat()
            remote_uat_copy_to_hdfs(to_path)
            clean_remote_uat_tmp()
            clean_local_dir()
        return True
    except Exception as e:
        log(f"❌ Transfer failed from {from_path} to {to_path}: {e}")
        return False

# === Entry Point ===
def main(param_file):
    success_list = []
    failure_list = []
    with open(param_file, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                from_path, to_path = line.split()
                if process_transfer(from_path, to_path):
                    success_list.append((from_path, to_path))
                else:
                    failure_list.append((from_path, to_path))
            except ValueError:
                log(f"Invalid line format at line {line_num}: {line}")
                failure_list.append((line, "Invalid format"))

    summary = "\n✅ Successful Transfers:\n" + "\n".join([f"{a} → {b}" for a, b in success_list])
    if failure_list:
        summary += "\n\n❌ Failed Transfers:\n" + "\n".join([f"{a} → {b}" for a, b in failure_list])
    else:
        summary += "\n\nAll transfers completed successfully."

    log("\n=== Transfer Summary ===\n" + summary)
    send_email("HDFS Transfer Summary", summary)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python hdfs_prod_to_uat_transfer.py <param_file>")
        sys.exit(1)
    main(sys.argv[1])
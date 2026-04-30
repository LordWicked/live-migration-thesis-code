#!/usr/bin/env python3

from dataclasses import dataclass
from pathlib import Path
import csv
import json
import socket
import subprocess
import time
import argparse
from typing import Optional, TextIO

@dataclass
class Config:
    image: Path
    overlay: Path
    runs: int
    log_path: Path
    src_port_base: int
    dst_port_base: int
    guest_user: str
    ssh_key: Path
    out_csv: Path
    mem_gb: int

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark QEMu+QMP live migration for a PostgreSQL VM"
    )

    parser.add_argument("image", type=Path, help="Base qcow2 image")
    # TODO Put overlay images fixed int/tmp or something
    parser.add_argument("overlay", type=Path, help="Path where overlay images will be stored.")
    parser.add_argument("--runs", type=int, default=10, help="Number of benchmark runs (default: 10)")
    parser.add_argument("--log-path", type=Path, default=Path("./logs"), help="Path to log files")
    parser.add_argument("--src-port-base", type=int, default=2222, help="Base host SSH port for source VM")
    parser.add_argument("--dst-port-base", type=int, default=4444, help="Base host SSH port for destination VM")
    parser.add_argument("--guest-user", type=str, default="user", help="Guest username for SSH")
    parser.add_argument( "--ssh-key", type=Path, default=Path(Path.home()/".ssh/pgvm_bench"), help="SSH private key path")
    parser.add_argument("--out-csv", type=Path, default=Path("./benchmarks/migration-benchmark-py.csv"), help="CSV output file")
    parser.add_argument("--mem-gb", type=int, default=4, help="Guest memory size in GiB")
    return parser

def create_overlay_image(base: Path, overlay: Path) -> None:
    if overlay.exists():
        overlay.unlink()
    print(base)
    print(overlay)
    subprocess.run([
        "qemu-img", "create", "-f", "qcow2", str(overlay), 
        "-o", f"backing_file={base},backing_fmt=qcow2"
    ], check=True)

def start_vm(overlay: Path, ssh_port: int, qmp_sock: Path, 
             log_file: TextIO, mem_gb: int, incoming_uri: Optional[str] = None) -> subprocess.Popen:
    cmd = ["qemu-system-x86_64",
           "-accel", "kvm",
           "-m", f"{mem_gb}G",
           "-smp", "4",
           "-cpu", "host",
           "-drive", f"file={overlay},if=virtio,format=qcow2",
           "-nic", f"user,hostfwd=tcp:127.0.0.1:{ssh_port}-:22",
           "-qmp", f"unix:{qmp_sock},server=on,wait=off",
           "-display", "none"
           ]
    if incoming_uri is not None:
        cmd.extend(["-incoming", incoming_uri])
    return subprocess.Popen(
        cmd,
        stdout=log_file,
        stderr=subprocess.STDOUT
    )

def cleanup(src_sock: Path, dst_sock: Path, src_log: TextIO, dst_log: TextIO) -> None:
    if src_log and not src_log.closed:
        src_log.close()
    if dst_log and not dst_log.closed:
        dst_log.close()
    if dst_sock.exists():
        qmp_cmd(dst_sock, "quit")
        dst_sock.unlink()
    if src_sock.exists():
        qmp_cmd(src_sock, "quit")
        src_sock.unlink()
            
def ssh_command(user: str, port: int, key: Path, remote_cmd: str, timeout: float) -> subprocess.CompletedProcess[str]:
    cmd = ["ssh", "-o", "BatchMode=yes", 
           "-o", "StrictHostKeyChecking=no", 
           "-o", "UserKnownHostsFile=/dev/null",
           "-o", "ConnectTimeout=1",
           "-p", str(port),
           "-i", str(key),
           f"{user}@127.0.0.1",
           remote_cmd]
    return subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)

def wait_for_return(user: str, port: int, key: Path, remote_cmd: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while True:
        if ssh_command(user, port, key, remote_cmd, 2.0).returncode == 0:
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Timed out waiting for SSH on port {port}")
        time.sleep(0.1)
        
def qmp_cmd(sock_path: Path, execute: str, arguments: dict | None = None) -> dict:
    if arguments is None:
        arguments = {}
        
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.connect(str(sock_path))
        
        greeting = s.recv(4096)
        
        # Capabilities
        s.sendall(json.dumps({"execute": "qmp_capabilities","id": "cap"}).encode() + b"\n")
        cap_reply = s.recv(4096)
        
        # Execute
        s.sendall(json.dumps({"execute": execute,"arguments": arguments, "id": "cmd"}).encode() + b"\n")
        exe_reply = s.recv(4096).decode()

        if "event" in exe_reply:
            return {"event": exe_reply}
        
    for line in exe_reply.splitlines():
        obj = json.loads(line)
        if obj.get("id") == "cmd":
            return obj
        
    raise RuntimeError(f"No QMP reply for {execute}")

def wait_for_status(sock_path: Path, timeout: float, goal_status: str) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            status = qmp_cmd(sock_path, "query-status")["return"]["status"]
            if  status == goal_status:
                return
        except Exception as e:
            pass
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Timed out waiting for {goal_status} status on socket {sock_path}")
        time.sleep(0.1)
    
def main() -> None:
    config = Config(**vars(build_parser().parse_args()))
    config.log_path.mkdir(parents=True, exist_ok=True)
    config.overlay.mkdir(parents=True, exist_ok=True)

    with open(config.out_csv, "w", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["run", "ssh_ready", "postgres_ready", "migration_start", "migration_end"])

    for run in range (1, config.runs + 1):
        print(f"Run {run}/{config.runs}")
        src_port = config.src_port_base + run - 1
        dst_port = config.dst_port_base + run - 1
        src_sock = Path(f"/tmp/src-pgvm-qmp-{run}.sock")
        dst_sock = Path(f"/tmp/dst-pgvm-qmp-{run}.sock")
        src_log = open(config.log_path/f"src-run-{run}.log", "w")
        dst_log = open(config.log_path/f"dst-run-{run}.log", "w")
        overlay=config.overlay / f"run{run}.qcow2"
        try:
            create_overlay_image(base=config.image, overlay=overlay)

            t_start = time.monotonic()
            src_vm = start_vm(overlay=overlay, ssh_port=src_port, qmp_sock=src_sock, log_file=src_log, mem_gb=config.mem_gb)
            dst_vm = start_vm(overlay=overlay, ssh_port=dst_port, qmp_sock=dst_sock, log_file=dst_log, mem_gb=config.mem_gb, incoming_uri="unix:/tmp/mig.sock")
            
            if src_vm.poll() is not None:
                raise RuntimeError("source VM exited immediately")
            wait_for_status(src_sock, 20.0, "running")
            if dst_vm.poll() is not None:
                raise RuntimeError("destination VM exited immediately")
            
            # Wait for SSH TODO and bench
            wait_for_return(user=config.guest_user, port=src_port, 
                            key=config.ssh_key, remote_cmd="true", timeout=60.0)
            t_ssh_ready = time.monotonic()

            wait_for_return(user=config.guest_user, port=src_port, 
                            key=config.ssh_key, remote_cmd="pg_isready -h 127.0.0.1 -p 5432 -q -t 1", timeout=60.0)
            t_postgres_ready = time.monotonic()

            # TODO Postgres payload
            

            # Start migration via QMP and measure time
            qmp_cmd(src_sock, "migrate", {"uri": "unix:/tmp/mig.sock"})
            t_migration_started = time.monotonic()

            # Wait for migration to finish and check result
            deadline = time.monotonic() + 120.0
            while True:
                status = qmp_cmd(src_sock, "query-migrate")["return"]["status"]
                if  status == "completed":
                    break
                elif status == "failed":
                    raise RuntimeError("Migration failed according to QMP")
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Timed out waiting for migration completion on socket {src_sock}")
                time.sleep(0.1)
            t_migration_completed = time.monotonic()

            # Append results to CSV
            with open(config.out_csv, "a", newline="") as csvfile:
                csv.writer(csvfile).writerow([
                    run, 
                    t_ssh_ready - t_start, 
                    t_postgres_ready - t_start, 
                    t_migration_started - t_start, 
                    t_migration_completed - t_start
                ])

        except:
            raise
        finally:
            cleanup(src_sock, dst_sock, src_log, dst_log)

if __name__ == "__main__":
    main()
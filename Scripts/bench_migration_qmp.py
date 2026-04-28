#!/usr/bin/env python3

from dataclasses import dataclass
from pathlib import Path
import csv
import json
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
    parser.add_argument("--out-csv", type=Path, default=Path("migration-benchmark.csv"), help="CSV output file")
    parser.add_argument("--mem-gb", type=int, default=4, help="Guest memory size in GiB")
    return parser

def create_overlay_image(base: Path, overlay: Path) -> None:
    if overlay.exists():
        overlay.unlink()
    print(f"Creating overlay image {overlay} based on {base}")
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

# TODO Kill VMS
def cleanup(src_sock: Path, dst_sock: Path, src_log: TextIO, dst_log: TextIO) -> None:
        if src_log and not src_log.closed:
            src_log.close()
        if dst_log and not dst_log.closed:
            dst_log.close()
        if src_sock.exists():
            src_sock.unlink()
        if dst_sock.exists():
            dst_sock.unlink()

def main() -> None:
    config = Config(**vars(build_parser().parse_args()))
    config.log_path.mkdir(parents=True, exist_ok=True)
    config.overlay.mkdir(parents=True, exist_ok=True)

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

            src_vm = start_vm(overlay=overlay, ssh_port=src_port, qmp_sock=src_sock, log_file=src_log, mem_gb=config.mem_gb)
            # Check after socket creation or sth, otherwise we might miss early exits
            if src_vm.poll() is not None:
                raise RuntimeError("source VM exited immediately")
            
            dst_vm = start_vm(overlay=overlay, ssh_port=dst_port, qmp_sock=dst_sock, log_file=dst_log, mem_gb=config.mem_gb, incoming_uri="unix:/tmp/mig.sock")
            # Check after socket creation or sth, otherwise we might miss early exits
            if dst_vm.poll() is not None:
                raise RuntimeError("destination VM exited immediately")
                    
            # TODO Wait for SSH and bench

            # TODO Postgres payload

            # TODO Start migration via QMP and measure time

            # TODO Wait for migration to finish and check result
        
        except:
            raise
        finally:
            cleanup(src_sock, dst_sock, src_log, dst_log)
        


if __name__ == "__main__":
    main()


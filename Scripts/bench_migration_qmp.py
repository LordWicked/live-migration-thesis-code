#!/usr/bin/env python3

from dataclasses import dataclass
from pathlib import Path
import csv
import json
import subprocess
import time
import argparse
import asyncio
from qemu.qmp import QMPClient
import psutil
from typing import Any, cast
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
    cores: int
    cpu: str
    record_count: int
    operation_count: int
    threads: int
    write: int
    read: int
    upd: int
    ins: int
    rm: int
    scan: int
    sleep_timer: float
    additional_args: Optional[str] = None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark QEMU+QMP live migration for a PostgreSQL VM"
    )

    parser.add_argument("image", type=Path, help="Base qcow2 image")
    # TODO Put overlay images fixed /var/tmp or something
    parser.add_argument("overlay", type=Path, help="Path where overlay images will be stored.")
    parser.add_argument("--runs", type=int, default=10, help="Number of benchmark runs (default: 10)")
    parser.add_argument("--log-path", type=Path, default=Path("./logs"), help="Path to log files")
    parser.add_argument("--src-port-base", type=int, default=2222, help="Base host SSH port for source VM")
    parser.add_argument("--dst-port-base", type=int, default=4444, help="Base host SSH port for destination VM")
    parser.add_argument("--guest-user", type=str, default="user", help="Guest username for SSH")
    parser.add_argument("--ssh-key", type=Path, default=Path(Path.home()/".ssh/pgvm_bench"), help="SSH private key path")
    parser.add_argument("--out-csv", type=Path, default=Path("./benchmarks/migration-benchmark-py.csv"), help="CSV output file")
    parser.add_argument("--mem-gb", type=int, default=4, help="Guest memory size in GiB")
    parser.add_argument("--cores", type=int, default=4, help="Amount of virtual CPU cores allocated to VM.")
    parser.add_argument("--cpu", type=str, default="host", help="Configure VM CPU model. host for host-passthrough.")
    # TODO maxmem etc. for potential migration target upgrade. Should be condition like incoming-uri in start_vm
    parser.add_argument("--record-count", type=int, default=2500000, help="Record count for the YCSB-A benchmark to be executed before migration.")
    parser.add_argument("--operation-count", type=int, default=1000000, help="Operation count for the YCSB-A benchmark to be executed before migration.")
    parser.add_argument("--threads", type=int, default=1, help="Thread count for PostgreSQL benchmark multithreading.")
    parser.add_argument("--write-proportion", type=int, default=1, help="Proportion of write operations for PostgreSQL YCSB benchmark. Default = 0")
    parser.add_argument("--read-proportion", type=int, default=0, help="Proportion of read operations for PostgreSQL YCSB benchmark. Default = 0")
    parser.add_argument("--update-proportion", type=int, default=0, help="Proportion of update operations for PostgreSQL YCSB benchmark. Default = 1")
    parser.add_argument("--insert-proportion", type=int, default=0, help="Proportion of insert operations for PostgreSQL YCSB benchmark. Default = 0")
    parser.add_argument("--readmodification-proportion", type=int, default=0, help="Proportion of readmodification operations for PostgreSQL YCSB benchmark. Default = 0")
    parser.add_argument("--scan-proportion", type=int, default=0, help="Proportion of scan operations for PostgreSQL YCSB benchmark. Default = 0")
    parser.add_argument("--sleep-timer", type=float, default=0.0, help="Introduce a wait period between benchmark and migration start in seconds")
    parser.add_argument("--additional-args", type=str, default=None, help="Additional QEMU command line arguments (i.e. for postcopy or CPU setup)")
    return parser

def create_overlay_image(base: Path, overlay: Path) -> None:
    if overlay.exists():
        overlay.unlink()
    subprocess.run([
        "qemu-img", "create", "-f", "qcow2", str(overlay), 
        "-o", f"backing_file={base},backing_fmt=qcow2"
    ], check=True)

def start_vm(overlay: Path, ssh_port: int, qmp_sock: Path, 
             log_file: TextIO, mem_gb: int, cores: int, cpu: str, incoming_uri: Optional[str] = None, additional_args: Optional[str] = None) -> subprocess.Popen:
    cmd = ["qemu-system-x86_64",
           "-accel", "kvm",
           "-m", f"{mem_gb}G",
           "-smp", f"{cores}",
           "-cpu", f"{cpu}",
           "-drive", f"file={overlay},if=virtio,format=qcow2",
           "-nic", f"user,hostfwd=tcp:127.0.0.1:{ssh_port}-:22",
           "-qmp", f"unix:{qmp_sock},server=on,wait=off",
           "-display", "none"
           ]
    if incoming_uri is not None:
        cmd.extend(["-incoming", incoming_uri])
    if additional_args is not None:
        cmd.extend(additional_args.split())
    return subprocess.Popen(
        cmd,
        stdout=log_file,
        stderr=log_file
    )
            
def ssh_command(user: str, port: int, key: Path, remote_cmd: str, timeout: float, log_file: TextIO | None = None) -> subprocess.CompletedProcess[str]:
    cmd = ["ssh", "-o", "BatchMode=yes", 
           "-o", "StrictHostKeyChecking=no", 
           "-o", "UserKnownHostsFile=/dev/null",
           "-o", "ConnectTimeout=1",
           "-p", str(port),
           "-i", str(key),
           f"{user}@127.0.0.1",
           remote_cmd]
    proc = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)

    if log_file is not None:
        log_file.write(f"SSH command: {' '.join(cmd)}\n")
        log_file.write(f"Return code: {proc.returncode}\n")
        log_file.write(f"Stdout: {proc.stdout}\n")
        log_file.write(f"Stderr: {proc.stderr}\n")
        log_file.flush()
    return proc

def wait_for_return(user: str, port: int, key: Path, remote_cmd: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while True:
        if ssh_command(user, port, key, remote_cmd, 2.0).returncode == 0:
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Timed out waiting for SSH on port {port}")
        time.sleep(0.1)

def scp_from_guest(user: str, ssh_port: int, ssh_key: Path, remote_path: str, local_path: Path, timeout: float = 30.0, log_file: TextIO | None = None) -> subprocess.CompletedProcess[str]:
    local_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "scp",
        "-P", str(ssh_port),
        "-i", str(ssh_key),
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        f"{user}@127.0.0.1:{remote_path}",
        str(local_path),
    ]

    proc = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
    if log_file is not None:
        log_file.write(f"SSH command: {' '.join(cmd)}\n")
        log_file.write(f"Return code: {proc.returncode}\n")
        log_file.write(f"Stdout: {proc.stdout}\n")
        log_file.write(f"Stderr: {proc.stderr}\n")
        log_file.flush()

    return subprocess.run(
        cmd,
        text=True,
        capture_output=True,
        timeout=timeout,
    )

class VMMonitor:
    def __init__(self, name, sock_path: Path, qmp_log: TextIO):
        self.name = name
        self.sock_path = sock_path
        self.qmp = QMPClient(name)
        self.qmp_log = qmp_log
        self.connected = False

    async def connect(self):
        await self.qmp.connect(str(self.sock_path)) 
        self.connected = True

    async def close(self):
        if self.connected:
            await self.quit()
            await self.qmp.disconnect()
            self.connected = False

    async def cmd(self, name: str, args: dict[str, Any] | None = None) -> dict[str, Any]:
        if args is None:
            args = {}
        self.qmp_log.write(f"QMP cmd [{self.name}] {name} args={args}\n")
        reply = cast(dict[str, Any], await self.qmp.execute(name, arguments=args))
        self.qmp_log.write(f"QMP reply [{self.name}] {name}: {reply}\n")
        return reply

    async def wait_for_qmp(self, proc: subprocess.Popen, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        error =  None
        while True:
            if proc.poll() is not None:
                raise RuntimeError(f"{self.name} exited before QMP was ready")
            try:
                await self.connect()
                return
            except Exception as e:
                error = e
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out waiting for QMP on {self.sock_path}: {error}")
    
    async def query_migrate(self): return await self.cmd("query-migrate")
    
    async def migrate(self, uri): 
        await self.cmd("migrate-set-capabilities", {"capabilities": [{"capability": "auto-converge", "state": False}]}) # TODO max-bandwidth ...
        return await self.cmd("migrate", {"uri":uri})
    
    async def quit(self): 
        # Forces shutdown, will trigger postgres WAL recovery
        return await self.cmd("quit")
    
async def cleanup(src: VMMonitor, src_vm: subprocess.Popen, dst: VMMonitor, dst_vm: subprocess.Popen, src_log: TextIO, dst_log: TextIO) -> None:
    await dst.close()
    await src.close()
    try:
        await asyncio.to_thread(src_vm.wait, timeout=20.0)
        await asyncio.to_thread(dst_vm.wait, timeout=20.0)
    except subprocess.TimeoutExpired:
        src_vm.kill()
        dst_vm.kill()
        await asyncio.to_thread(src_vm.wait)
        await asyncio.to_thread(dst_vm.wait)

    # Path("/tmp/mig.sock").unlink()

    if src_log and not src_log.closed:
        src_log.close()
    if dst_log and not dst_log.closed:
        dst_log.close()

async def main() -> None:
    config = Config(**vars(build_parser().parse_args()))
    config.log_path.mkdir(parents=True, exist_ok=True)
    config.overlay.mkdir(parents=True, exist_ok=True)

    with open(config.out_csv, "w", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["run", "ssh_ready", "postgres_ready", "benchmark_start", "migration_start", "migration_end", "benchmark_end"])

    t_0 = time.monotonic()

    for run in range (1, config.runs + 1):
        print(f"Run {run}/{config.runs}")
        src_port = config.src_port_base + run - 1
        dst_port = config.dst_port_base + run - 1
        src_sock = Path(f"/tmp/src-pgvm-qmp-{run}.sock")
        dst_sock = Path(f"/tmp/dst-pgvm-qmp-{run}.sock")
        slog_path = config.log_path/f"src-run-{run}.log"
        dlog_path = config.log_path/f"dst-run-{run}.log"
        if slog_path.exists(): slog_path.unlink()
        if dlog_path.exists(): dlog_path.unlink()
        src_log = open(config.log_path/f"src-run-{run}.log", "a")
        dst_log = open(config.log_path/f"dst-run-{run}.log", "a")
        mig_log = open(config.log_path/f"mig-stats-run-{run}.json", "a")
        src = VMMonitor(f"src{run}", src_sock, src_log)
        dst = VMMonitor(f"dst{run}", dst_sock, dst_log)
        overlay = config.overlay / f"run{run}.qcow2"
        
        create_overlay_image(base=config.image, overlay=overlay)
        t_start = time.monotonic()
        print(f"Time elapsed: {t_start - t_0}")
        try:
            src_vm = start_vm(overlay=overlay, ssh_port=src_port, qmp_sock=src_sock, log_file=src_log, mem_gb=config.mem_gb, cores=config.cores, cpu=config.cpu)
            dst_vm = start_vm(overlay=overlay, ssh_port=dst_port, qmp_sock=dst_sock, log_file=dst_log, mem_gb=config.mem_gb, cores=config.cores, cpu=config.cpu, incoming_uri="unix:/tmp/mig.sock")
            
            await src.wait_for_qmp(src_vm, 20)
            await dst.wait_for_qmp(dst_vm, 20)
            
            # Wait for SSH and bench
            wait_for_return(user=config.guest_user, port=src_port, 
                            key=config.ssh_key, remote_cmd="true", timeout=60.0)
            t_ssh_ready = time.monotonic()

            wait_for_return(user=config.guest_user, port=src_port, 
                            key=config.ssh_key, remote_cmd="pg_isready -h 127.0.0.1 -p 5432 -q -t 1", timeout=60.0)
            t_postgres_ready = time.monotonic()

            ssh_command(user=config.guest_user, port=src_port, key=config.ssh_key, remote_cmd="systemd-analyze", timeout=10.0, log_file=src_log)

            # Postgres payload (needs to return yscb-a run output)
            bench_command = f"cd ycsb-0.17.0; bin/ycsb.sh run  jdbc -P workloads/workloada -P db.properties -p recordcount={config.record_count} -p operationcount={config.operation_count} -threads {config.threads} -s -p status.interval=2 -p readproportion={config.read} -p updateproportion={config.upd} -p insertproportion={config.ins} -p scanproportion={config.scan} -p readmodifywriteproportion={config.rm}"
            ssh_command(user=config.guest_user, 
                        port=src_port, 
                        key=config.ssh_key,
                        remote_cmd=(f"nohup bash -c '{bench_command}; touch /tmp/bench.done' "
                                    ">/tmp/bench.log 2>&1 &"), 
                        timeout=10.0,
                        log_file=dst_log) # TODO Correct log?
            t_bench_started = time.monotonic()

            time.sleep (config.sleep_timer)

            # Start migration via QMP and measure time
            await src.migrate("unix:/tmp/mig.sock")
            t_migration_started = time.monotonic()

            # Wait for migration and benchmark to finish
            json_data = []
            deadline = time.monotonic() + 300.0
            migration_done = False
            bench_done = False
            t_migration_completed = 0.0
            t_bench_completed = 0.0
            warning = False
            while True:
                now = time.monotonic()

                if not migration_done:
                    mig = await src.query_migrate()
                    json_data.append(mig)
                    status = mig["status"]
                    if  status == "completed":
                        migration_done = True
                        t_migration_completed = now
                    elif status == "failed":
                        raise RuntimeError("Migration failed according to QMP")
                    
                if not bench_done:
                    port = dst_port if migration_done else src_port
                    log = dst_log if migration_done else src_log
                    try:
                        proc = ssh_command(user=config.guest_user, port=port, key=config.ssh_key, remote_cmd="test -f /tmp/bench.done", timeout=2.0, log_file=log)
                        if proc.returncode == 0:
                            bench_done = True
                            t_bench_completed = now
                    except subprocess.TimeoutExpired:
                        pass

                if migration_done and bench_done:
                    break

                if not warning and now >= deadline:
                    if bench_done and not migration_done:
                        # raise TimeoutError(f"Timed out waiting for migration completion on socket {src_sock}.")
                        print(f"Timed out waiting for migration completion on socket {src_sock}.\nTerminate this process manually if this is unexpected.")
                    if not bench_done and not migration_done:
                        # raise TimeoutError(f"Timed out waiting for migration and benchmark completion on socket {src_sock}.")
                        print(f"Timed out waiting for migration and benchmark completion on socket {src_sock}.\nTerminate this process manually if this is unexpected.")
                    if not bench_done and migration_done:
                        # raise TimeoutError(f"Timed out waiting for benchmark completion on socket {dst_sock}.")
                        print(f"Timed out waiting for benchmark completion on socket {dst_sock}.\nTerminate this process manually if this is unexpected.")
                    warning = True

                time.sleep(0.2)

            scp_from_guest(user=config.guest_user, ssh_port=dst_port, ssh_key=config.ssh_key, remote_path="/tmp/bench.log" ,local_path=config.log_path/f"bench-run-{run}", log_file=dst_log)
            
            find_pg_log = ssh_command(user=config.guest_user, port=dst_port, key=config.ssh_key, remote_cmd="ls -1t /var/log/postgresql/postgres*.json 2>/dev/null | head -n1", timeout=5, log_file=dst_log)
            scp_from_guest(user=config.guest_user, ssh_port=dst_port, ssh_key=config.ssh_key, remote_path=find_pg_log.stdout.strip() ,local_path=config.log_path/f"postgres-run-{run}.json", log_file=dst_log)
            
            # Append results to CSV
            mig_log.write(json.dumps(json_data))
            with open(config.out_csv, "a", newline="") as csvfile:
                csv.writer(csvfile).writerow([
                    run, 
                    t_ssh_ready - t_start, 
                    t_postgres_ready - t_start, 
                    t_bench_started - t_start,
                    t_migration_started - t_start, 
                    t_migration_completed - t_start,
                    t_bench_completed - t_start
                ])

        except Exception as e:
            raise e
        finally:
            await cleanup(src, src_vm, dst, dst_vm, src_log, dst_log) # type: ignore
    print(f"Total time elapsed: {time.monotonic()-t_0}")

if __name__ == "__main__":
    asyncio.run(main())
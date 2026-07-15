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
import datetime
import re
from typing import Any, cast
from typing import Optional, TextIO
from host_resource_sampler import HostResourceSampler
from event_logger import EventLogger

@dataclass
class Config:
    image: Path
    overlay: Path
    runs: int
    socket_path: Path
    log_path: Path
    src_port_base: int
    dst_port_base: int
    guest_user: str
    ssh_key: Path
    out_csv: Path
    mem_gb: int
    cores: int
    cpu: str
    migration_mode: int
    record_count: int
    operation_count: int
    threads: int
    write_proportion_dep: float
    read_proportion: float
    update_proportion: float
    insert_proportion: float
    readmodification_proportion: float
    scan_proportion: float
    sleep_timer: float
    auto_converge: int
    postcopy_sleep: int


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark QEMU+QMP live migration for a PostgreSQL VM"
    )

    parser.add_argument("image", type=Path, help="Base qcow2 image")
    # TODO Put overlay images fixed /var/tmp or something
    parser.add_argument("overlay", type=Path, help="Path where overlay images will be stored.")
    parser.add_argument("--runs", type=int, default=10, help="Number of benchmark runs (default: 10)")
    parser.add_argument("--socket-path", type=Path, default=Path("/tmp/"), help="Change socket path. Default: /tmp/")
    parser.add_argument("--log-path", type=Path, default=Path("./logs"), help="Path to log files")
    parser.add_argument("--src-port-base", type=int, default=2222, help="Base host SSH port for source VM")
    parser.add_argument("--dst-port-base", type=int, default=4444, help="Base host SSH port for destination VM")
    parser.add_argument("--guest-user", type=str, default="user", help="Guest username for SSH")
    parser.add_argument("--ssh-key", type=Path, default=Path(Path.home()/".ssh/pgvm_bench"), help="SSH private key path")
    parser.add_argument("--out-csv", type=Path, default=Path("./benchmarks/migration-benchmark-py.csv"), help="CSV output file")
    parser.add_argument("--mem-gb", type=int, default=4, help="Guest memory size in GiB")
    parser.add_argument("--cores", type=int, default=4, help="Amount of virtual CPU cores allocated to VM.")
    parser.add_argument("--cpu", type=str, default="host", help="Configure VM CPU model. host for host-passthrough.")
    parser.add_argument("--migration-mode", type=int, default=0, help="Sets migration mode. 0: precopy (default), 1: stop-copy, 2: postcopy")
    # TODO maxmem etc. for potential migration target upgrade. Should be condition like incoming-uri in start_vm
    parser.add_argument("--record-count", type=int, default=2500000, help="Record count for the YCSB-A benchmark to be executed before migration.")
    parser.add_argument("--operation-count", type=int, default=1000000, help="Operation count for the YCSB-A benchmark to be executed before migration.")
    parser.add_argument("--threads", type=int, default=1, help="Thread count for PostgreSQL benchmark multithreading.")
    parser.add_argument("--write-proportion-dep", type=float, default=0, help="Not compatible with YCSB-A, will fail. Proportion of write operations for PostgreSQL YCSB benchmark. Default = 0")
    parser.add_argument("--read-proportion", type=float, default=0, help="Proportion of read operations for PostgreSQL YCSB benchmark. Default = 0")
    parser.add_argument("--update-proportion", type=float, default=1, help="Proportion of update operations for PostgreSQL YCSB benchmark. Default = 1")
    parser.add_argument("--insert-proportion", type=float, default=0, help="Proportion of insert operations for PostgreSQL YCSB benchmark. Default = 0")
    parser.add_argument("--readmodification-proportion", type=float, default=0, help="Proportion of readmodification operations for PostgreSQL YCSB benchmark. Default = 0")
    parser.add_argument("--scan-proportion", type=float, default=0, help="Proportion of scan operations for PostgreSQL YCSB benchmark. Default = 0")
    parser.add_argument("--sleep-timer", type=float, default=0.0, help="Introduce a wait period between benchmark and migration start in seconds")
    parser.add_argument("--auto-converge", type=int, default=0, help="Sets auto-converge. 0: off (default), 1: on")
    parser.add_argument("--postcopy-sleep", type=int, default=0, help="Timer for postcopy to wait after precopy has been called. Default: 0.0")
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
           "-o", "LogLevel=ERROR",
           "-p", str(port),
           "-i", str(key),
           f"{user}@127.0.0.1",
           remote_cmd]
    proc = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)

    ct = datetime.datetime.now()
    if log_file is not None:
        log_file.write(f"{ct}")
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
    ct = datetime.datetime.now()
    proc = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
    if log_file is not None:
        log_file.write(f"{ct}")
        log_file.write(f"SSH command: {' '.join(cmd)}\n")
        log_file.write(f"Return code: {proc.returncode}\n")
        log_file.write(f"Stdout: {proc.stdout}\n")
        log_file.write(f"Stderr: {proc.stderr}\n")
        log_file.flush()
    return proc

def start_pg_buffer_sampler(user: str, port: int, key: Path, log_file: TextIO) -> subprocess.CompletedProcess[str]:
    return ssh_command(
        user=user,
        port=port,
        key=key,
        remote_cmd=(
            "rm -f /tmp/bench.done /tmp/pg-buffer-hit-ratio.csv /tmp/pg-buffer-sampler.log; "
            "nohup bash /home/user/pg_sampler.sh /tmp/bench.done /tmp/pg-buffer-hit-ratio.csv ycsb "
            ">/tmp/pg-buffer-sampler.log 2>&1 &"
        ),
        timeout=10.0,
        log_file=log_file,
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
            try:
                await self.qmp.disconnect()
            finally:
                self.connected = False

    async def cmd(self, name: str, args: dict[str, Any] | None = None) -> dict[str, Any]:
        if args is None:
            args = {}
        self.qmp_log.write(f"QMP cmd [{self.name}] {name} args={args}\n")
        try:
            reply = cast(dict[str, Any], await self.qmp.execute(name, arguments=args))
            self.qmp_log.write(f"QMP reply [{self.name}] {name}: {reply}\n")
            return reply
        except Exception as e:
            self.qmp_log.write(f"QMP error [{self.name}] {name}: {e}\n")
            self.qmp_log.flush()
            # Mark as disconnected if we get a state error
            if "disconnected" in str(e).lower() or "StateError" in str(type(e).__name__):
                self.connected = False
            raise

    async def wait_for_qmp(self, proc: subprocess.Popen, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        error =  None
        while True:
            # print(self.sock_path)
            if proc.poll() is not None:
                raise RuntimeError(f"{self.name} exited before QMP was ready")
            try:
                await self.connect()
                return
            except Exception as e:
                error = e
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out waiting for QMP on {self.sock_path}: {error}")
            time.sleep(0.1)
    
    async def query_migrate(self): return await self.cmd("query-migrate")
    
    async def migrate(self, uri, mode, converge): 
        """Mode: 0 = precopy | 1 = postcopy"""
        await self.cmd("migrate-set-capabilities", 
                       {"capabilities": [{"capability": "auto-converge", "state": converge != 0}, 
                                         {"capability": "events", "state": True},
                                         {"capability": "postcopy-blocktime", "state": mode == 1},
                                         {"capability": "postcopy-ram", "state": mode == 1}]})
        return await self.cmd("migrate", {"uri":uri})
        
    async def postcopy_start(self):
        return await self.cmd("migrate-start-postcopy")

    
    async def stop(self): return await self.cmd("stop")

    async def cont(self): return await self.cmd("cont")
    
    async def quit(self): 
        """Forces shutdown, will trigger postgres WAL recovery"""
        return await self.cmd("quit")
                
async def poll_migration(src: VMMonitor, dst: VMMonitor, t_start: float, json_data: list, stop_copy: bool, postcopy: bool, postcopy_sleep: float, postcopy_status: asyncio.Event, event_logger: EventLogger) -> float:
    now = 0.0
    postcopy_sleep = time.monotonic() + postcopy_sleep
    postcopy_requested = False
    while True:
        now = time.monotonic()
        mig = await src.query_migrate()
        json_data.append({
            "t_monotonic": now,
            "t_since_run_start": now - t_start,
            "query_migrate": mig,
        })

        status = mig.get("status")

        if postcopy and not postcopy_requested and status == "active" and time.monotonic() >= postcopy_sleep:
            await src.postcopy_start()
            event_logger.mark("postcopy_requested")
            postcopy_requested = True

        if not postcopy_status.is_set() and status == 'postcopy-active': 
            event_logger.mark("postcopy_started")
            postcopy_status.set()

        if status == "completed":
            event_logger.mark("migration_end")
            if stop_copy: 
                await dst.cont()
                event_logger.mark("destination_resumed")
            return now
        
        if status == "failed": raise RuntimeError("Migration failed according to QMP")

        await asyncio.sleep(0.2) # was 0.1
                
async def poll_hugepage(dst_vm: subprocess.Popen) -> subprocess.CompletedProcess[str]:
    smaps_path = f"/proc/{dst_vm.pid}/smaps_rollup"

    hugepages = subprocess.run(
        [
            "grep",
            "-E",
            "^(Rss|Pss|AnonHugePages):",
            smaps_path,
        ],
        text=True, capture_output=True, timeout=5,
    )

    if not hugepages.returncode == 0:
        print(
            f"Failed to read {smaps_path}: "
            f"{hugepages.stderr.strip()}"
        ) 
    return hugepages
        

async def repeated_hp_poll(dst_vm: subprocess.Popen, log: Path):
    timings = [0, 10, 30, 60, 120]
    with open(log, "w") as hp_log:
        for t in timings:
            if hp_log.closed:
                return
            else:
                await asyncio.sleep(t)
                proc = await poll_hugepage(dst_vm=dst_vm)
                hp_log.write(f"{t}: {proc.stdout}\n\n")
        

async def close_monitor(monitor: VMMonitor):
    try:
        await monitor.close()
    except Exception as e:
        monitor.qmp_log.write(f"QMP close for monitor [{monitor.name}] failed with: {e}\n")      
        monitor.qmp_log.flush()

async def stop_vm_proc(proc: subprocess.Popen, name: str, log_file: TextIO):
    if proc.poll() is not None: return
    try:
        await asyncio.to_thread(proc.wait, timeout=10.0)
        return
    except subprocess.TimeoutExpired:
        log_file.write(f"Cleanup terminating {name} pid={proc.pid}\n")
        log_file.flush()

    proc.terminate()
    try:
        await asyncio.to_thread(proc.wait, timeout=10.0)
        return
    except subprocess.TimeoutExpired:
        log_file.write(f"Cleanup killing {name} pid={proc.pid}\n")
        log_file.flush()

    proc.kill()
    await asyncio.to_thread(proc.wait)

async def cleanup(src: VMMonitor, src_vm: subprocess.Popen, dst: VMMonitor, dst_vm: subprocess.Popen, src_log: TextIO, dst_log: TextIO) -> None:
    try: 
        await close_monitor(src)
        await close_monitor(dst)
        await stop_vm_proc(src_vm, src.name, src_log)
        await stop_vm_proc(dst_vm, dst.name, dst_log)
        
    finally:
        if src_log and not src_log.closed:
            src_log.close()
        if dst_log and not dst_log.closed:
            dst_log.close()
        if Path("/tmp/mig.sock").exists():
            Path("/tmp/mig.sock").unlink()

STATUS_RE = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}:\d{3}) "
    r"(?P<elapsed>\d+) sec: "
    r"(?P<ops>\d+) operations; "
    r"(?P<ops_per_sec>[0-9.]+) current ops/sec;"
)

LATENCY_RE = re.compile(r"\[[A-Z-]+: ([^\]]+)\]")

def ycsb_status_to_csv(log_path: Path, csv_path: Path) -> None:
    with log_path.open("r", encoding="utf-8", errors="replace") as infile, \
         csv_path.open("w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["timestamp", "elapsed_sec", "total_operations", "current_ops_per_sec", "avg_latency_us", "max_latency_us",
    "p99_latency_us", "p999_latency_us", "p9999_latency_us"])

        for line in infile:
            m = STATUS_RE.match(line.strip())
            if not m:
                continue

            counts = 0
            avg_total = 0.0
            max_latency = p99 = p999 = p9999 = ""

            for stats_text in LATENCY_RE.findall(line):
                stats = dict(item.split("=", 1) for item in stats_text.split(", "))
                count = int(stats.get("Count", 0))
                if not count:
                    continue

                counts += count
                avg_total += count * float(stats.get("Avg", 0))
                max_latency = max(int(max_latency or 0), int(stats.get("Max", 0)))
                p99 = max(int(p99 or 0), int(stats.get("99", 0)))
                p999 = max(int(p999 or 0), int(stats.get("99.9", 0)))
                p9999 = max(int(p9999 or 0), int(stats.get("99.99", 0)))

            writer.writerow([
                m.group("timestamp"),
                int(m.group("elapsed")),
                int(m.group("ops")),
                float(m.group("ops_per_sec")),
                (avg_total / counts) if counts else "",
                max_latency,
                p99,
                p999,
                p9999,
            ])

async def main() -> None:
    config = Config(**vars(build_parser().parse_args()))
    config.log_path.mkdir(parents=True, exist_ok=True)
    config.overlay.mkdir(parents=True, exist_ok=True)

    BOOT_AT_START = False

    # if (config.migration_mode == 2): config.socket_path = Path("/home/max/Bachelor-Thesis/VMs/postgresvm/sockets/")

    with open(config.out_csv, "w", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["run", "ssh_ready", "postgres_ready", "benchmark_start", "destination_boot", "migration_start", "migration_end", "benchmark_end"])

    t_0 = time.monotonic()

    for run in range (1, config.runs + 1):
        print(f"Run {run}/{config.runs}")
        src_port = config.src_port_base + run - 1
        dst_port = config.dst_port_base + run - 1
        src_sock = Path(f"{config.socket_path}/src-pgvm-qmp-{run}.sock")
        dst_sock = Path(f"{config.socket_path}/dst-pgvm-qmp-{run}.sock")
        if src_sock.exists(): src_sock.unlink()
        if dst_sock.exists(): dst_sock.unlink()
        slog_path = config.log_path/f"src-run-{run}.log"
        dlog_path = config.log_path/f"dst-run-{run}.log"
        if slog_path.exists(): slog_path.unlink()
        if dlog_path.exists(): dlog_path.unlink()
        src_log = open(config.log_path/f"src-run-{run}.log", "w")
        dst_log = open(config.log_path/f"dst-run-{run}.log", "w")
        # mig_log = open(config.log_path/f"mig-stats-run-{run}.json", "w")
        src = VMMonitor(f"src{run}", src_sock, src_log)
        dst = VMMonitor(f"dst{run}", dst_sock, dst_log)
        overlay = config.overlay / f"run{run}.qcow2"
        
        create_overlay_image(base=config.image, overlay=overlay)

        t_start = time.monotonic()

        src_vm = subprocess.Popen(["true"])
        dst_vm = subprocess.Popen(["true"])

        host_sampler = HostResourceSampler(
            csv_path=config.log_path/f"host-stats-run-{run}.csv",
            run_start=t_start,
            src_proc_getter=lambda: src_vm,
            dst_proc_getter=lambda: dst_vm,
        )
        event_logger = EventLogger(
            csv_path=(
                config.log_path
                / f"events-run-{run}.csv"
            ),
            run_start=t_start,
        )
        event_logger.mark("run_start")
        host_sampler.start()

        print(f"Time elapsed: {t_start - t_0}")

        migration_poll_task: asyncio.Task[float] | None = None

        try:
            t_destination_boot = 0.0
            src_vm = start_vm(overlay=overlay, ssh_port=src_port, qmp_sock=src_sock, log_file=src_log, mem_gb=config.mem_gb, cores=config.cores, cpu=config.cpu)
            
            event_logger.mark(
                "source_vm_started",
                f"pid={src_vm.pid}",
            )

            await src.wait_for_qmp(src_vm, 20)

            event_logger.mark("source_qmp_ready")
        
            if BOOT_AT_START:
                t_destination_boot = time.monotonic()
                event_logger.mark("destination_boot_start")
                dst_vm = start_vm(overlay=overlay, ssh_port=dst_port, qmp_sock=dst_sock, log_file=dst_log, mem_gb=config.mem_gb, cores=config.cores, cpu=config.cpu, incoming_uri="unix:/tmp/mig.sock")
                event_logger.mark(
                    "destination_process_started",
                    f"pid={dst_vm.pid}",
                )
                await dst.wait_for_qmp(dst_vm, 20)
                event_logger.mark("destination_qmp_ready")
            
            
            # Wait for SSH and bench
            wait_for_return(user=config.guest_user, port=src_port, 
                            key=config.ssh_key, remote_cmd="true", timeout=60.0)
            t_ssh_ready = time.monotonic()
            event_logger.mark("source_ssh_ready")

            wait_for_return(user=config.guest_user, port=src_port, 
                            key=config.ssh_key, remote_cmd="pg_isready -h 127.0.0.1 -p 5432 -q -t 1", timeout=60.0)
            t_postgres_ready = time.monotonic()
            event_logger.mark("source_postgres_ready")

            ssh_command(user=config.guest_user, port=src_port, key=config.ssh_key, remote_cmd="systemd-analyze", timeout=10.0, log_file=src_log)

            # Postgres payload (needs to return yscb-a run output)
            # timeout 200
            if config.read_proportion == 1.0:
                bench_command = f"cd ycsb-0.17.0 && timeout 200 bin/ycsb.sh run  jdbc -P workloads/workloada -P db.properties -p recordcount={config.record_count} -p operationcount={config.operation_count} -threads {config.threads} -s -p status.interval=1 -p writeproportion={config.write_proportion_dep} -p readproportion={config.read_proportion} -p updateproportion={config.update_proportion} -p insertproportion={config.insert_proportion} -p scanproportion={config.scan_proportion} -p readmodifywriteproportion={config.readmodification_proportion}"
            else:
                bench_command = f"cd ycsb-0.17.0 && bin/ycsb.sh run  jdbc -P workloads/workloada -P db.properties -p recordcount={config.record_count} -p operationcount={config.operation_count} -threads {config.threads} -s -p status.interval=1 -p writeproportion={config.write_proportion_dep} -p readproportion={config.read_proportion} -p updateproportion={config.update_proportion} -p insertproportion={config.insert_proportion} -p scanproportion={config.scan_proportion} -p readmodifywriteproportion={config.readmodification_proportion}"
            start_pg_buffer_sampler(user=config.guest_user, port=src_port, key=config.ssh_key, log_file=src_log)
            ssh_command(user=config.guest_user, 
                        port=src_port, 
                        key=config.ssh_key,
                        remote_cmd=(f"nohup bash -c '{bench_command}; touch /tmp/bench.done' "
                                    ">/tmp/bench.log 2>&1 &"), 
                        timeout=10.0,
                        log_file=dst_log)
            event_logger.mark("benchmark_start")
            t_bench_started = time.monotonic()

            time.sleep (config.sleep_timer)

            if not BOOT_AT_START:
                t_destination_boot = time.monotonic()
                event_logger.mark("destination_boot_start")
                dst_vm = start_vm(overlay=overlay, ssh_port=dst_port, qmp_sock=dst_sock, log_file=dst_log, mem_gb=config.mem_gb, cores=config.cores, cpu=config.cpu, incoming_uri="unix:/tmp/mig.sock")
                event_logger.mark(
                    "destination_process_started",
                    f"pid={dst_vm.pid}",
                )
                await dst.wait_for_qmp(dst_vm, 20)
                event_logger.mark("destination_qmp_ready")

            # Start migration via QMP and measure time
            json_data = []
            postcopy_started = asyncio.Event()
            t_migration_completed = 0.0
            migration_done = False 
            if config.migration_mode == 1:
                t_migration_started = time.monotonic()
                migration_poll_task = asyncio.create_task(poll_migration(src=src, dst=dst, t_start=t_start, json_data=json_data, stop_copy=True, postcopy=False, postcopy_sleep=0.0, postcopy_status=postcopy_started, event_logger=event_logger))
                await src.stop()
                event_logger.mark("source_stopped")
                await src.migrate("unix:/tmp/mig.sock", 0, config.auto_converge)
                event_logger.mark("migration_started")
            elif config.migration_mode == 2:
                await dst.cmd("migrate-set-capabilities", 
                       {"capabilities": [{"capability": "postcopy-ram", "state": True},
                                         {"capability": "postcopy-blocktime", "state": True},
                                         {"capability": "events", "state": True}]})
                migration_poll_task = asyncio.create_task(poll_migration(src=src, dst=dst, t_start=t_start, json_data=json_data, stop_copy=False, postcopy=True, postcopy_sleep=config.postcopy_sleep, postcopy_status=postcopy_started, event_logger=event_logger))
                t_migration_started = time.monotonic()
                await src.migrate("unix:/tmp/mig.sock", 1, config.auto_converge)
                event_logger.mark("migration_started")
            else:
                migration_poll_task = asyncio.create_task(poll_migration(src=src, dst=dst, t_start=t_start, json_data=json_data, stop_copy=False, postcopy=False, postcopy_sleep=0.0, postcopy_status=postcopy_started, event_logger=event_logger))
                t_migration_started = time.monotonic()
                await src.migrate("unix:/tmp/mig.sock", 0, config.auto_converge)
                event_logger.mark("migration_started")

                
            # Wait for migration and benchmark to finish
            deadline = time.monotonic() + 300.0
            bench_done = False
            t_bench_completed = 0.0
            warning = False
            while True:
                now = time.monotonic()

                if not migration_done and migration_poll_task.done():
                    t_migration_completed = migration_poll_task.result()
                    migration_done = True

                    if config.migration_mode == 2:
                        # with open(config.log_path/f"hugepages-{run}.log", "w") as log:
                        asyncio.create_task(repeated_hp_poll(dst_vm=dst_vm, log=Path(f"{config.log_path}/hugepages-{run}.log")))
                        # smaps_path = f"/proc/{dst_vm.pid}/smaps_rollup"

                        # hugepages = subprocess.run(
                        #     [
                        #         "grep",
                        #         "-E",
                        #         "^(Rss|Pss|AnonHugePages):",
                        #         smaps_path,
                        #     ],
                        #     text=True, capture_output=True, timeout=5,
                        # )

                        # if hugepages.returncode == 0:
                        #     print(hugepages.stdout)
                        # else:
                        #     print(
                        #         f"Failed to read {smaps_path}: "
                        #         f"{hugepages.stderr.strip()}"
                        #     )
                    try: 
                        event_logger.mark(
                            "source_terminate_requested",
                            f"pid={src_vm.pid}",
                        )
                        src_vm.terminate()
                        await close_monitor(src)
                        await stop_vm_proc(src_vm, src.name, src_log)
                        event_logger.mark(
                            "source_terminated",
                            f"pid={src_vm.pid}",
                        )
                    except subprocess.SubprocessError as e:
                        raise e
                    
                if not bench_done:
                    guest_on_dst = migration_done or postcopy_started.is_set()
                    port = dst_port if guest_on_dst else src_port
                    log = dst_log if guest_on_dst else src_log
                    try:
                        proc = await asyncio.to_thread(ssh_command, user=config.guest_user, port=port, key=config.ssh_key, remote_cmd="test -f /tmp/bench.done", timeout=2.0, log_file=log)
                        if proc.returncode == 0:
                            event_logger.mark("benchmark_end")
                            bench_done = True
                            t_bench_completed = now
                    except subprocess.TimeoutExpired:
                        pass

                if migration_done and bench_done:
                    break

                if not warning and now >= deadline:
                    if bench_done and not migration_done:
                        print(f"Timed out waiting for migration completion on socket {src_sock}.\nTerminate this process manually if this is unexpected.")
                    if not bench_done and not migration_done:
                        print(f"Timed out waiting for migration and benchmark completion on socket {src_sock}.\nTerminate this process manually if this is unexpected.")
                    if not bench_done and migration_done:
                        print(f"Timed out waiting for benchmark completion on socket {dst_sock}.\nTerminate this process manually if this is unexpected.")
                    warning = True

                await asyncio.sleep(2) # was 0.1

            scp_from_guest(user=config.guest_user, ssh_port=dst_port, ssh_key=config.ssh_key, remote_path="/tmp/bench.log" ,local_path=config.log_path/f"bench-run-{run}", log_file=dst_log)
            scp_from_guest(user=config.guest_user, ssh_port=dst_port, ssh_key=config.ssh_key, remote_path="/tmp/pg-buffer-hit-ratio.csv" ,local_path=config.log_path/f"bench-run-{run}-pgstats.csv", log_file=dst_log)
            
            find_pg_log = ssh_command(user=config.guest_user, port=dst_port, key=config.ssh_key, remote_cmd="ls -1t /var/log/postgresql/postgres*.json 2>/dev/null | head -n1", timeout=5, log_file=dst_log)
            scp_from_guest(user=config.guest_user, ssh_port=dst_port, ssh_key=config.ssh_key, remote_path=find_pg_log.stdout.strip() ,local_path=config.log_path/f"postgres-run-{run}.json", log_file=dst_log)
            
            # Append results to CSV
            now = time.monotonic()
            mig = await dst.query_migrate()
            json_data.append({
                "t_monotonic": now,
                "t_since_run_start": now - t_start,
                "query_migrate": mig,
            })


            with open(config.log_path/f"mig-stats-run-{run}.json", "w") as mig_log:
                mig_log.write(json.dumps(json_data))
            with open(config.out_csv, "a", newline="") as csvfile:
                csv.writer(csvfile).writerow([
                    run, 
                    t_ssh_ready - t_start, 
                    t_postgres_ready - t_start, 
                    t_bench_started - t_start,
                    t_destination_boot - t_start,
                    t_migration_started - t_start, 
                    t_migration_completed - t_start,
                    t_bench_completed - t_start
                ])

            ycsb_status_to_csv(log_path=config.log_path/f"bench-run-{run}", csv_path=config.log_path/f"bench-run-{run}.csv")

        except Exception as e:
            raise e
        finally:
            if migration_poll_task is not None and not migration_poll_task.done():
                migration_poll_task.cancel()
                try:
                    await migration_poll_task
                except asyncio.CancelledError:
                    pass
            if host_sampler is not None: await host_sampler.stop()
            if event_logger is not None: event_logger.close()
            await cleanup(src, src_vm, dst, dst_vm, src_log, dst_log)
    print(f"Total time elapsed: {time.monotonic()-t_0}")

if __name__ == "__main__":
    asyncio.run(main())

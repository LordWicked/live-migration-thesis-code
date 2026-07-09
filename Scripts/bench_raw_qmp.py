#!/usr/bin/env python3

from dataclasses import dataclass
from pathlib import Path
import csv
import subprocess
import time
import argparse
import asyncio
from qemu.qmp import QMPClient
import datetime
import re
import shlex
from typing import Any, cast
from typing import Optional, TextIO
from host_resource_sampler import HostResourceSampler
from event_logger import EventLogger

@dataclass
class Config:
    image: Path
    overlay: Path
    socket_naming: str
    restart: int
    prepare_restart: int
    standby_image: Path
    prewarm: int
    prewarm_image: Path
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
    write_proportion_dep: float
    read_proportion: float
    update_proportion: float
    insert_proportion: float
    readmodification_proportion: float
    scan_proportion: float
    sleep_timer: float
    additional_args: Optional[str] = None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark QEMU+QMP live migration for a PostgreSQL VM"
    )

    parser.add_argument("image", type=Path, help="Base qcow2 image")
    # TODO Put overlay images fixed /var/tmp or something
    parser.add_argument("overlay", type=Path, help="Path where overlay images will be stored.")
    parser.add_argument("--socket-naming", type=str, default='', help="Add prefix to socket naming to avoid contention.")
    parser.add_argument("--restart", type=int, default=0, help="Toggle restarting the VM during benchmark. Default=0")
    parser.add_argument("--prepare-restart", type=int, default=0, help="Prepare the destination VM for switchover instead of shutting 1 down and only then booting 2. Beware of extra boot time immediately after sleep if enabled. Default=0")
    parser.add_argument("--standby-image", type=Path, default=None, help="Standby image for the prepared restart destination.")
    parser.add_argument("--prewarm", type=int, default=0, help="Prepare the destination with warm RAM state from source. Default=0")
    parser.add_argument("--prewarm-image", type=Path, default=None, help="Base prewarmed or streaming (for prepared restart) image. Needed if using prewarm.")
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
    parser.add_argument("--record-count", type=int, default=2500000, help="Record count for the YCSB-A benchmark to be executed before migration.")
    parser.add_argument("--operation-count", type=int, default=100000, help="Operation count for the YCSB-A benchmark to be executed before migration.")
    parser.add_argument("--threads", type=int, default=1, help="Thread count for PostgreSQL benchmark multithreading.")
    parser.add_argument("--write-proportion-dep", type=float, default=0, help="Not compatible with YCSB-A, will fail. Proportion of write operations for PostgreSQL YCSB benchmark. Default = 0")
    parser.add_argument("--read-proportion", type=float, default=0, help="Proportion of read operations for PostgreSQL YCSB benchmark. Default = 0")
    parser.add_argument("--update-proportion", type=float, default=1, help="Proportion of update operations for PostgreSQL YCSB benchmark. Default = 1")
    parser.add_argument("--insert-proportion", type=float, default=0, help="Proportion of insert operations for PostgreSQL YCSB benchmark. Default = 0")
    parser.add_argument("--readmodification-proportion", type=float, default=0, help="Proportion of readmodification operations for PostgreSQL YCSB benchmark. Default = 0")
    parser.add_argument("--scan-proportion", type=float, default=0, help="Proportion of scan operations for PostgreSQL YCSB benchmark. Default = 0")
    parser.add_argument("--sleep-timer", type=float, default=0.0, help="Introduce a wait period between benchmark and restart in seconds")
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
             log_file: TextIO, mem_gb: int, cores: int, cpu: str, source: Optional[bool] = False, incoming_uri: Optional[str] = None, additional_args: Optional[str] = None) -> subprocess.Popen:
    cmd = ["qemu-system-x86_64",
           "-accel", "kvm",
           "-m", f"{mem_gb}G",
           "-smp", f"{cores}",
           "-cpu", f"{cpu}",
           "-drive", f"file={overlay},if=virtio,format=qcow2",
           "-nic", f"user,hostfwd=tcp:127.0.0.1:{ssh_port}-:22{",hostfwd=tcp:127.0.0.1:5433-:5432" if source else ""}",
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

def wait_for_standby(user: str, port: int, key: Path, log_file: TextIO, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while True:
        result = ssh_command(user=user, port=port, key=key, 
                             remote_cmd= ("sudo -n -u postgres "
                                          "psql -d postgres -tA -c \""
                                          "SELECT "
                                          "pg_last_wal_receive_lsn() IS NOT NULL "
                                          "AND pg_last_wal_replay_lsn() IS NOT NULL "
                                          "AND pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn();"
                                          "\""), 
                            timeout=10.0, log_file=log_file)
        if result.returncode == 0 and result.stdout.strip() == "t":
            return
        
        if time.monotonic() >= deadline:
            raise TimeoutError("Standby did not finish replaying received WAL")
        
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
            time.sleep(0.1)
            
    async def powerdown(self): return await self.cmd("system_powerdown")
    
    async def quit(self): 
        return await self.cmd("quit")
    
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

    with open(config.out_csv, "w", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["run", "ssh_ready", "postgres_ready", "standby_ready", "promotion_start", "promotion_done", "benchmark_resume", "benchmark_start", "benchmark_end", "shutdown_request", "shutdown_finished", "ssh2_ready", "postgres2_ready"])
        
    t_0 = time.monotonic()

    for run in range (1, config.runs + 1):
        print(f"Run {run}/{config.runs}")
        src_port = config.src_port_base + run - 1
        dst_port = config.dst_port_base + run - 1
        src_sock = Path(f"/tmp/{config.socket_naming}src-pgvm-qmp-{run}.sock")
        dst_sock = Path(f"/tmp/{config.socket_naming}src-pgvm-qmp-reboot-{run}.sock")
        if src_sock.exists(): src_sock.unlink()
        if dst_sock.exists(): dst_sock.unlink()
        src_log = open(config.log_path/f"src-run-{run}.log", "a")
        dst_log = open(config.log_path/f"src-run-{run}-reboot.log", "a")
        src = VMMonitor(f"src{run}", src_sock, src_log)
        dst = VMMonitor(f"src{run}_reboot", dst_sock, dst_log)
        overlay = config.overlay / f"{config.socket_naming}run{run}.qcow2"
        dst_overlay = config.overlay / f"{config.socket_naming}dst_run{run}.qcow2"
        
        t_start = time.monotonic()

        src_vm = subprocess.Popen(["true"])
        dst_vm = subprocess.Popen(["true"])

        host_sampler = HostResourceSampler(
            csv_path=config.log_path/f"host-stats-run-{run}.csv",
            run_start=t_start,
            src_proc_getter=lambda: src_vm,
            dst_proc_getter=lambda: dst_vm,
        )
        host_sampler.start()
        event_logger = EventLogger(
            csv_path=(
                config.log_path
                / f"events-run-{run}.csv"
            ),
            run_start=t_start,
        )
        event_logger.mark("run_start")

        print(f"Time elapsed: {t_start - t_0}")

        try:
            if config.prewarm == 0: create_overlay_image(base=config.image, overlay=overlay)
            else: create_overlay_image(base=config.prewarm_image, overlay=overlay)
            if config.prepare_restart == 1: 
                if config.standby_image is None:
                    raise ValueError("--standby-image is required with --prepare-restart 1")
                create_overlay_image(base=config.standby_image, overlay=dst_overlay)

            src_vm = start_vm(overlay=overlay, ssh_port=src_port, qmp_sock=src_sock, log_file=src_log, mem_gb=config.mem_gb, cores=config.cores, cpu=config.cpu, source=True)
            
            event_logger.mark(
                "source_vm_started",
                f"pid={src_vm.pid}",
            )

            await src.wait_for_qmp(src_vm, 20)

            event_logger.mark("source_qmp_ready")
            
            # Wait for SSH and bench
            wait_for_return(user=config.guest_user, port=src_port, 
                            key=config.ssh_key, remote_cmd="true", timeout=60.0)
            t_ssh_ready = time.monotonic()
            event_logger.mark("ssh_ready")

            wait_for_return(user=config.guest_user, port=src_port, 
                            key=config.ssh_key, remote_cmd="pg_isready -h 127.0.0.1 -p 5432 -q -t 1", timeout=60.0)
            t_postgres_ready = time.monotonic()
            event_logger.mark("postgres_ready")
            
            ssh_command(user=config.guest_user, port=src_port, key=config.ssh_key, remote_cmd="systemd-analyze", timeout=10.0, log_file=src_log)

            # Postgres payload (needs to return yscb-a run output)
            bench_command = f"cd ycsb-0.17.0; bin/ycsb.sh run  jdbc -P workloads/workloada -P db.properties -p recordcount={config.record_count} -p operationcount={config.operation_count} -threads {config.threads} -s -p status.interval=1 -p writeproportion={config.write_proportion_dep} -p readproportion={config.read_proportion} -p updateproportion={config.update_proportion} -p insertproportion={config.insert_proportion} -p scanproportion={config.scan_proportion} -p readmodifywriteproportion={config.readmodification_proportion}"
            start_pg_buffer_sampler(user=config.guest_user, port=src_port, key=config.ssh_key, log_file=src_log)
            ssh_command(user=config.guest_user, 
                        port=src_port, 
                        key=config.ssh_key,
                        remote_cmd=(f"nohup bash -c '{bench_command}; touch /tmp/bench.done' "
                                    ">/tmp/bench.log 2>&1 &"), 
                        timeout=10.0,
                        log_file=src_log)
            event_logger.mark("source_benchmark_start")
            t_bench_started = time.monotonic()

            event_logger.mark("benchmark_start")
            
            t_shutdown_request = 0.0
            t_power_off = 0.0
            t_ssh2_ready = 0.0
            t_pg2_ready = 0.0
            t_dst_warm_start = 0.0
            t_standby_ready = 0.0
            t_promotion_start = 0.0
            t_promotion_done = 0.0
            t_benchmark_resume = 0.0

            port_actual = dst_port if config.prepare_restart else src_port
            
            t_ssh2_ready = time.monotonic()

            if(config.restart):
                time.sleep(config.sleep_timer)  

                if config.prepare_restart:
                    event_logger.mark("destination_boot_start")
                    dst_vm = start_vm(overlay=dst_overlay, ssh_port=dst_port, qmp_sock=dst_sock, log_file=dst_log, mem_gb=config.mem_gb, cores=config.cores, cpu=config.cpu)
                    event_logger.mark("destination_process_started", f"pid={dst_vm.pid}")
                    await dst.wait_for_qmp(dst_vm, 20)
                    event_logger.mark("destination_qmp_ready")

                    wait_for_return(user=config.guest_user, port=dst_port, key=config.ssh_key, remote_cmd="true", timeout=60.0)
                    t_ssh2_ready = time.monotonic()
                    event_logger.mark("destination_ssh_ready")

                    wait_for_return(user=config.guest_user, port=dst_port, key=config.ssh_key, remote_cmd="pg_isready -h 127.0.0.1 -p 5432 -q -t 1", timeout=120.0)
                    t_pg2_ready = time.monotonic()
                    event_logger.mark("destination_standby_postgres_ready")

                    standby = ssh_command(user=config.guest_user, port=dst_port, key=config.ssh_key,
                        remote_cmd=(
                            "sudo -n -u postgres "
                            "psql -d postgres -tA "
                            "-c 'SELECT pg_is_in_recovery();'"
                        ),
                        timeout=10.0,
                        log_file=dst_log,
                    )
                    if standby.stdout.strip() != "t":
                        raise RuntimeError(
                            "Prepared destination is not a standby"
                        )
                    
                    # Wait for wal to catch up
                    deadline = time.monotonic() + 300.0
                    while True:
                        lag = ssh_command(user=config.guest_user, port=src_port, key=config.ssh_key,
                            remote_cmd=(
                                "sudo -n -u postgres "
                                "psql -d postgres -tA -c \""
                                "SELECT COALESCE("
                                "pg_wal_lsn_diff("
                                "pg_current_wal_flush_lsn(), "
                                "replay_lsn"
                                "), -1)::bigint "
                                "FROM pg_stat_replication "
                                "WHERE state = 'streaming' "
                                "LIMIT 1;\""
                            ), timeout=10.0, log_file=src_log,
                        )

                        if lag.returncode != 0:
                            raise RuntimeError(
                                "Replication lag query failed: "
                                f"{lag.stderr.strip()}"
                            )

                        try:
                            lag_bytes = int(lag.stdout.strip())
                        except ValueError:
                            lag_bytes = -1

                        if 0 <= lag_bytes <= 16 * 1024 * 1024:
                            break

                        if time.monotonic() >= deadline:
                            raise TimeoutError(
                                "Standby did not catch up"
                            )

                        time.sleep(0.5)
                    event_logger.mark(
                        "standby_ready",
                        f"lag_bytes={lag_bytes}",
                    )
                    t_standby_ready = time.monotonic()

                    scp_from_guest(user=config.guest_user, ssh_port=src_port, ssh_key=config.ssh_key, remote_path="/tmp/bench.log", local_path=config.log_path/f"bench-run-{run}-preshut", log_file=src_log)
                    scp_from_guest(user=config.guest_user, ssh_port=src_port, ssh_key=config.ssh_key, remote_path="/tmp/pg-buffer-hit-ratio.csv", local_path=config.log_path/f"bench-run-{run}-preshut-pgstats.csv", log_file=src_log)
                    scp_from_guest(user=config.guest_user, ssh_port=src_port, ssh_key=config.ssh_key, remote_path="/tmp/pg-buffer-sampler.log", local_path=config.log_path/f"bench-run-{run}-preshut-pgstats.log", log_file=src_log)
                    find_pg_log = ssh_command(user=config.guest_user, port=src_port, key=config.ssh_key, remote_cmd="ls -1t /var/log/postgresql/postgres*.json 2>/dev/null | head -n1", timeout=5, log_file=src_log)
                    if find_pg_log.stdout.strip():
                        scp_from_guest(user=config.guest_user, ssh_port=src_port, ssh_key=config.ssh_key, remote_path=find_pg_log.stdout.strip(), local_path=config.log_path/f"postgres-run-{run}-preshut.json", log_file=src_log)
                    
                    event_logger.mark("source_shutdown_request")
                    t_shutdown_request = time.monotonic()
                    await src.powerdown()
                    try:
                        await asyncio.to_thread(
                            src_vm.wait,
                            timeout=120.0,
                        )
                    except subprocess.TimeoutExpired:
                        raise TimeoutError(
                            "Source VM did not shut down cleanly"
                        )
                    t_power_off = time.monotonic()
                    event_logger.mark("source_powered_off")

                    # Wait until WAL is replayed
                    event_logger.mark("standby_replay_drain_start")
                    wait_for_standby(user=config.guest_user, port=dst_port, key=config.ssh_key, log_file=dst_log, timeout=60.0)
                    event_logger.mark("standby_replay_drained") 

                    # Promote replayed DB
                    t_promotion_start = time.monotonic()
                    event_logger.mark("promotion_start")
                    promotion = ssh_command(user=config.guest_user, port=dst_port, key=config.ssh_key,
                        remote_cmd=(
                            "sudo -n -u postgres "
                            "psql -d postgres "
                            "-c 'SELECT pg_promote(wait => true);'"
                        ), timeout=60.0, log_file=dst_log,
                    )
                    if promotion.returncode != 0:
                        raise RuntimeError(f"Standby promotion failed: {promotion.stderr}")
                    event_logger.mark("promotion_complete")
                    t_promotion_done = time.monotonic()
                    
                    wait_for_return(user=config.guest_user, port=dst_port, key=config.ssh_key,
                        remote_cmd=(
                            "sudo -n -u postgres "
                            "psql -d postgres -tA "
                            "-c 'SELECT NOT pg_is_in_recovery();' "
                            "| grep -q t"
                        ), timeout=60.0,
                    )
                else:
                    scp_from_guest(user=config.guest_user, ssh_port=src_port, ssh_key=config.ssh_key, remote_path="/tmp/bench.log" ,local_path=config.log_path/f"bench-run-{run}-preshut", log_file=src_log)
                    scp_from_guest(user=config.guest_user, ssh_port=src_port, ssh_key=config.ssh_key, remote_path="/tmp/pg-buffer-hit-ratio.csv" ,local_path=config.log_path/f"bench-run-{run}-preshut-pgstats.csv", log_file=src_log)
                    scp_from_guest(user=config.guest_user, ssh_port=src_port, ssh_key=config.ssh_key, remote_path="/tmp/pg-buffer-sampler.log" ,local_path=config.log_path/f"bench-run-{run}-preshut-pgstats.log", log_file=src_log)
                    find_pg_log = ssh_command(user=config.guest_user, port=src_port, key=config.ssh_key, remote_cmd="ls -1t /var/log/postgresql/postgres*.json 2>/dev/null | head -n1", timeout=5, log_file=src_log)
                    scp_from_guest(user=config.guest_user, ssh_port=src_port, ssh_key=config.ssh_key, remote_path=find_pg_log.stdout.strip() ,local_path=config.log_path/f"postgres-run-{run}-preshut.json", log_file=src_log)

                    if config.prewarm and not config.prepare_restart:
                        ssh_command(
                            user=config.guest_user, port=src_port, key=config.ssh_key,
                            remote_cmd="sudo -n -u postgres psql -v ON_ERROR_STOP=1 -d postgres -c 'SELECT autoprewarm_dump_now();'",
                            timeout=30.0, log_file=src_log,
                        )

                    t_shutdown_request = time.monotonic()
                    event_logger.mark("source_powerdown_requested")
                    await src.powerdown()
                    try:
                        await asyncio.to_thread(src_vm.wait, timeout=120.0)
                    except subprocess.TimeoutExpired:
                        raise TimeoutError("Guest did not shut down cleanly after system_powerdown.")
                    t_power_off = time.monotonic()
                    event_logger.mark("source_powered_off")
                        
                    # if not config.prepare_restart:
                    event_logger.mark("destination_boot_start")
                    dst_vm = start_vm(overlay=overlay, ssh_port=src_port, qmp_sock=dst_sock, log_file=src_log, mem_gb=config.mem_gb, cores=config.cores, cpu=config.cpu)
                    event_logger.mark(
                        "destination_process_started",
                        f"pid={dst_vm.pid}",
                    )
                    await dst.wait_for_qmp(dst_vm, 20)
                    event_logger.mark("destination_qmp_ready")

                    wait_for_return(user=config.guest_user, port=src_port, key=config.ssh_key, remote_cmd="true", timeout=60.0)
                    t_ssh2_ready = time.monotonic()

                    wait_for_return(user=config.guest_user, port=port_actual, key=config.ssh_key, remote_cmd="pg_isready -h 127.0.0.1 -p 5432 -q -t 1", timeout=120.0)
                    t_pg2_ready = time.monotonic()
                    event_logger.mark("destination_postgres_ready")

                start_pg_buffer_sampler(user=config.guest_user, port=port_actual, key=config.ssh_key, log_file=src_log)
                ssh_command(user=config.guest_user, port=port_actual, key=config.ssh_key, 
                            remote_cmd=(f"nohup bash -c '{bench_command}; touch /tmp/bench.done' "
                                    ">/tmp/bench.log 2>&1 &"), timeout=10.0, log_file=src_log)
                t_benchmark_resume = time.monotonic()
                event_logger.mark("destination_benchmark_start")
            

            # Wait for benchmark to finish
            deadline = time.monotonic() + 300.0
            bench_done = False
            t_bench_completed = 0.0
            warning = False
            while True:
                now = time.monotonic()
                    
                if not bench_done:
                    try:
                        proc = ssh_command(user=config.guest_user, port=port_actual, key=config.ssh_key, remote_cmd="test -f /tmp/bench.done", timeout=2.0, log_file=src_log)
                        if proc.returncode == 0:
                            event_logger.mark("benchmark_end")
                            bench_done = True
                            t_bench_completed = now
                    except subprocess.TimeoutExpired:
                        pass

                if bench_done:
                    break

                if not warning and now >= deadline:
                    if not bench_done:
                        print(f"Timed out waiting for benchmark completion on socket {src_sock}.\nTerminate this process manually if this is unexpected.")
                    warning = True

                time.sleep(2)

            scp_from_guest(user=config.guest_user, ssh_port=port_actual, ssh_key=config.ssh_key, remote_path="/tmp/bench.log" ,local_path=config.log_path/f"bench-run-{run}-final", log_file=src_log)
            scp_from_guest(user=config.guest_user, ssh_port=port_actual, ssh_key=config.ssh_key, remote_path="/tmp/pg-buffer-hit-ratio.csv" ,local_path=config.log_path/f"bench-run-{run}-final-pgstats.csv", log_file=src_log)
            scp_from_guest(user=config.guest_user, ssh_port=port_actual, ssh_key=config.ssh_key, remote_path="/tmp/pg-buffer-sampler.log" ,local_path=config.log_path/f"bench-run-{run}-final-pgstats.log", log_file=src_log)
            
            find_pg_log = ssh_command(user=config.guest_user, port=port_actual, key=config.ssh_key, remote_cmd="ls -1t /var/log/postgresql/postgres*.json 2>/dev/null | head -n1", timeout=5, log_file=src_log)
            scp_from_guest(user=config.guest_user, ssh_port=port_actual, ssh_key=config.ssh_key, remote_path=find_pg_log.stdout.strip() ,local_path=config.log_path/f"postgres-run-{run}-final.json", log_file=src_log)
            
            # Append results to CSV
            with open(config.out_csv, "a", newline="") as csvfile:
                csv.writer(csvfile).writerow([
                    run, 
                    t_ssh_ready - t_start, 
                    t_postgres_ready - t_start, 
                    t_standby_ready - t_start,
                    t_promotion_start - t_start,
                    t_promotion_done - t_start,
                    t_benchmark_resume - t_start,
                    t_bench_started - t_start,
                    t_bench_completed - t_start,
                    t_shutdown_request - t_start if config.restart else 0.0, 
                    t_power_off - t_start if config.restart else 0.0,
                    t_ssh2_ready - t_start if config.restart else 0.0,
                    t_pg2_ready - t_start if config.restart else 0.0
                ])

            ycsb_status_to_csv(log_path=config.log_path/f"bench-run-{run}-final", csv_path=config.log_path/f"bench-run-{run}-final.csv")
            if Path(config.log_path/f"bench-run-{run}-preshut").exists():
                ycsb_status_to_csv(log_path=config.log_path/f"bench-run-{run}-preshut", csv_path=config.log_path/f"bench-run-{run}-preshut.csv")

        except Exception as e:
            raise e
        finally:
            if host_sampler is not None: await host_sampler.stop()
            if event_logger is not None: event_logger.close()
            await cleanup(src, src_vm, dst, dst_vm, src_log, dst_log)
    print(f"Total time elapsed: {time.monotonic()-t_0}")


if __name__ == "__main__":
    asyncio.run(main())

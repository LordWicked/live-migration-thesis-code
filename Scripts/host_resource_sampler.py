from pathlib import Path
import asyncio
import csv
import subprocess
import threading
import time

import psutil


PSI_KINDS = ("cpu", "memory", "io")
PSI_STALL_TYPES = ("some", "full")
PSI_FIELDS = ("avg10", "avg60", "avg300", "total")


class HostResourceSampler:
    def __init__(
        self,
        csv_path: Path,
        run_start: float,
        src_proc_getter,
        dst_proc_getter,
        interval: float = 1.0,
    ):
        self.csv_path = csv_path
        self.run_start = run_start
        self.src_proc_getter = src_proc_getter
        self.dst_proc_getter = dst_proc_getter
        self.interval = interval

        # Independent of the asyncio event loop
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

        self._processes: dict[int, psutil.Process] = {}
        self._last_process_io: dict[
            str,
            tuple[int, float, int, int]
        ] = {}
        self._last_disk = None
        self._last_disk_time = 0.0

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("Host resource sampler already started")

        # Prime psutil's interval-based counters.
        self._last_disk = psutil.disk_io_counters()
        self._last_disk_time = time.monotonic()

        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="host-resource-sampler",
            daemon=True,
        )
        self._thread.start()

    async def stop(self) -> None:
        self._stop.set()

        if self._thread is not None:
            # Do not block the asyncio loop while waiting for the thread.
            await asyncio.to_thread(self._thread.join)
            self._thread = None

    def _run(self) -> None:
        psutil.cpu_percent(interval=None)

        self.csv_path.parent.mkdir(parents=True, exist_ok=True)

        with self.csv_path.open(
            "w",
            newline="",
            encoding="utf-8",
        ) as csvfile:
            writer = csv.DictWriter(
                csvfile,
                fieldnames=self._fieldnames(),
            )
            writer.writeheader()

            while not self._stop.is_set():
                writer.writerow(self._sample())
                csvfile.flush()

                # Wait independently of asyncio.
                self._stop.wait(self.interval)

    def _fieldnames(self) -> list[str]:
        fields = [
            "t_monotonic",
            "t_since_run_start",
            "timestamp_unix",
            "src_pid",
            "src_cpu_percent",
            "src_rss_bytes",
            "src_read_bytes",
            "src_write_bytes",
            "src_read_bytes_per_s",
            "src_write_bytes_per_s",
            "dst_pid",
            "dst_cpu_percent",
            "dst_rss_bytes",
            "dst_read_bytes",
            "dst_write_bytes",
            "dst_read_bytes_per_s",
            "dst_write_bytes_per_s",
            "system_cpu_percent",
            "mem_available_bytes",
            "swap_used_bytes",
            "host_dirty_bytes",
            "host_writeback_bytes",
            "disk_read_bytes_per_s",
            "disk_write_bytes_per_s",
        ]

        for kind in PSI_KINDS:
            for stall_type in PSI_STALL_TYPES:
                for field in PSI_FIELDS:
                    fields.append(
                        f"psi_{kind}_{stall_type}_{field}"
                    )

        return fields

    def _sample(self) -> dict[str, object]:
        now = time.monotonic()

        row: dict[str, object] = {
            "t_monotonic": now,
            "t_since_run_start": now - self.run_start,
            "timestamp_unix": time.time(),
            "system_cpu_percent": psutil.cpu_percent(interval=None),
            "mem_available_bytes": psutil.virtual_memory().available,
            "swap_used_bytes": psutil.swap_memory().used,
        }

        row.update(
            self._process_metrics(
                "src",
                self.src_proc_getter(),
                now,
            )
        )
        row.update(
            self._process_metrics(
                "dst",
                self.dst_proc_getter(),
                now,
            )
        )
        row.update(self._host_writeback_metrics())
        row.update(self._disk_rates(now))
        row.update(self._psi_metrics())

        return row

    def _process_metrics(
        self,
        prefix: str,
        proc: subprocess.Popen | None,
        now: float,
    ) -> dict[str, object]:
        row: dict[str, object] = {
            f"{prefix}_pid": "",
            f"{prefix}_cpu_percent": "",
            f"{prefix}_rss_bytes": "",

            f"{prefix}_read_bytes": "",
            f"{prefix}_write_bytes": "",
            f"{prefix}_read_bytes_per_s": "",
            f"{prefix}_write_bytes_per_s": "",
        }

        # Process does not exist or has already exited.
        if proc is None or proc.poll() is not None:
            self._last_process_io.pop(prefix, None)
            return row

        row[f"{prefix}_pid"] = proc.pid

        try:
            process = self._processes.get(proc.pid)

            if process is None:
                process = psutil.Process(proc.pid)

                # Prime CPU measurement.
                process.cpu_percent(interval=None)

                self._processes[proc.pid] = process

            # CPU and memory
            row[f"{prefix}_cpu_percent"] = (
                process.cpu_percent(interval=None)
            )
            row[f"{prefix}_rss_bytes"] = (
                process.memory_info().rss
            )

            # Cumulative process I/O
            io = process.io_counters()

            current_read = io.read_bytes
            current_write = io.write_bytes

            row[f"{prefix}_read_bytes"] = current_read
            row[f"{prefix}_write_bytes"] = current_write

            # Derive per-second rates from consecutive samples.
            previous = self._last_process_io.get(prefix)

            if previous is not None:
                (
                    previous_pid,
                    previous_time,
                    previous_read,
                    previous_write,
                ) = previous

                # Only compare samples belonging to the same process.
                if previous_pid == proc.pid:
                    elapsed = max(
                        now - previous_time,
                        1e-9,
                    )

                    row[f"{prefix}_read_bytes_per_s"] = (
                        max(
                            0,
                            current_read - previous_read,
                        )
                        / elapsed
                    )

                    row[f"{prefix}_write_bytes_per_s"] = (
                        max(
                            0,
                            current_write - previous_write,
                        )
                        / elapsed
                    )

            self._last_process_io[prefix] = (
                proc.pid,
                now,
                current_read,
                current_write,
            )

        except (
            psutil.NoSuchProcess,
            psutil.AccessDenied,
            psutil.ZombieProcess,
        ):
            self._processes.pop(proc.pid, None)
            self._last_process_io.pop(prefix, None)

        return row

    def _host_writeback_metrics(self) -> dict[str, object]:
        metrics: dict[str, object] = {
            "host_dirty_bytes": "",
            "host_writeback_bytes": "",
        }

        try:
            for line in Path("/proc/meminfo").read_text(
                encoding="utf-8"
            ).splitlines():
                key, value = line.split(":", 1)

                if key == "Dirty":
                    # /proc/meminfo reports KiB.
                    metrics["host_dirty_bytes"] = (
                        int(value.split()[0]) * 1024
                    )

                elif key == "Writeback":
                    metrics["host_writeback_bytes"] = (
                        int(value.split()[0]) * 1024
                    )

        except (OSError, ValueError, IndexError):
            pass

        return metrics

    def _disk_rates(
        self,
        now: float,
    ) -> dict[str, float]:
        current = psutil.disk_io_counters()
        
        previous = self._last_disk
        previous_time = self._last_disk_time

        self._last_disk = current
        self._last_disk_time = now

        if current is None or previous is None:
            return {
                "disk_read_bytes_per_s": 0.0,
                "disk_write_bytes_per_s": 0.0,
            }

        elapsed = max(now - previous_time, 1e-9)

        return {
            "disk_read_bytes_per_s":
                max(0, current.read_bytes - previous.read_bytes)
                / elapsed,
            "disk_write_bytes_per_s":
                max(0, current.write_bytes - previous.write_bytes)
                / elapsed,
        }

    def _psi_metrics(self) -> dict[str, object]:
        metrics: dict[str, object] = {
            f"psi_{kind}_{stall_type}_{field}": ""
            for kind in PSI_KINDS
            for stall_type in PSI_STALL_TYPES
            for field in PSI_FIELDS
        }

        for kind in PSI_KINDS:
            path = Path(f"/proc/pressure/{kind}")

            if not path.exists():
                continue

            try:
                for line in path.read_text(
                    encoding="utf-8"
                ).splitlines():
                    parts = line.split()

                    if not parts:
                        continue

                    stall_type = parts[0]

                    for item in parts[1:]:
                        key, value = item.split("=", 1)

                        metrics[
                            f"psi_{kind}_{stall_type}_{key}"
                        ] = (
                            float(value)
                            if key != "total"
                            else int(value)
                        )

            except OSError:
                continue

        return metrics
from pathlib import Path
import csv
import threading
import time


class EventLogger:
    def __init__(
        self,
        csv_path: Path,
        run_start: float,
    ):
        self.csv_path = csv_path
        self.run_start = run_start

        self._lock = threading.Lock()

        self.csv_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._csvfile = self.csv_path.open(
            "w",
            newline="",
            encoding="utf-8",
        )

        self._writer = csv.DictWriter(
            self._csvfile,
            fieldnames=[
                "t_monotonic",
                "t_since_run_start",
                "timestamp_unix",
                "event",
                "details",
            ],
        )

        self._writer.writeheader()
        self._csvfile.flush()

    def mark(
        self,
        event: str,
        details: str = "",
    ) -> None:
        now = time.monotonic()

        row = {
            "t_monotonic": now,
            "t_since_run_start": (
                now - self.run_start
            ),
            "timestamp_unix": time.time(),
            "event": event,
            "details": details,
        }

        with self._lock:
            self._writer.writerow(row)
            self._csvfile.flush()

    def close(self) -> None:
        with self._lock:
            if not self._csvfile.closed:
                self._csvfile.close()
#!/usr/bin/env python3
"""Build run-level and scenario-level evaluation tables from thesis logs.

Version: 2026-07-21-final-v2

This script is tailored to the final Bachelor-thesis log layout containing:
- baseline scenarios,
- QEMU migration scenarios,
- stop-copy,
- cold/prewarmed restart,
- prepared PostgreSQL standby handover.

Outputs:
- evaluation_runs.csv          one row per experimental run
- evaluation_summary.csv       single long-format aggregate table for all metrics
- data_quality.csv             validity and missing-diagnostic overview
- metric_definitions.csv       definitions and units used by the script

The run is the statistical unit. Time-window means are first calculated per run;
scenario statistics are then calculated across those run-level values.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import re
import shutil
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

VERSION = "2026-07-21-final-v2"

# ---------------------------------------------------------------------------
# Analysis windows. Change these centrally if the final methodology changes.
# ---------------------------------------------------------------------------
PRE_EVENT_WINDOW_S = 20.0
PRE_THROTTLE_WINDOW_S = 10.0
POST_EARLY_END_S = 10.0
POST_MID_START_S = 10.0
POST_MID_END_S = 30.0
POST_LATE_START_S = 30.0
POST_LATE_END_S = 90.0
BASELINE_STEADY_WINDOWS_S = {
    "Baseline_4T": (80.0, 300.0),
    "Baseline_16T": (80.0, 180.0),
    "Baseline_16T_OnlyReads": (80.0, 180.0),
}
PREPARED_SOURCE_ALONE_WINDOW_S = 20.0
PREPARED_EARLY_DEST_END_S = 40.0
PREPARED_LATE_DEST_START_S = 60.0
PREPARED_LATE_DEST_END_S = 120.0

BYTES_PER_MIB = 1024**2
BYTES_PER_GIB = 1024**3
GUEST_RAM_GIB = 8.0
HUGEPAGE_SAMPLE_TIMES_S = (0, 10, 40, 100)

POSTCOPY_SCENARIOS = {
    "Postcopy_4T",
    "Postcopy_16T",
    "Postcopy_Late_16T_5s",
    "Postcopy_Late_16T_25s",
    "Postcopy_Late_16T_40s",
}
HUGEPAGE_SCENARIOS = POSTCOPY_SCENARIOS
ACTIVE_QMP_STATUSES = {"active", "postcopy-active"}

INPUT_FILE_PATTERNS = (
    "specs_*",
    "events-run-*.csv",
    "bench-run-*.csv",
    "mig-stats-run-*.json",
    "host-stats-run-*.csv",
    "hugepages-*.log",
    "postgres-run-*.json",
)

AGGREGATION_METHOD = (
    "The experimental run is the statistical unit. Per-run values are aggregated "
    "across valid runs using arithmetic mean, sample SD (ddof=1), median, pandas "
    "linear-interpolation quartiles, minimum, and maximum."
)


@dataclass(frozen=True)
class ScenarioSpec:
    name: str
    kind: str
    threads: int
    workload: str
    expected_operations: int | None
    timed_workload: bool = False


SCENARIOS: dict[str, ScenarioSpec] = {
    "Baseline_4T": ScenarioSpec("Baseline_4T", "baseline", 4, "100% updates", 250_000),
    "Baseline_16T": ScenarioSpec("Baseline_16T", "baseline", 16, "100% updates", 500_000),
    "Baseline_16T_OnlyReads": ScenarioSpec(
        "Baseline_16T_OnlyReads", "baseline", 16, "100% reads", None, True
    ),
    "Precopy_Convergent": ScenarioSpec(
        "Precopy_Convergent", "migration", 4, "100% updates", 250_000
    ),
    "Postcopy_4T": ScenarioSpec("Postcopy_4T", "migration", 4, "100% updates", 250_000),
    "Stop_Copy_4T": ScenarioSpec("Stop_Copy_4T", "stopcopy", 4, "100% updates", 250_000),
    "Precopy_Nonconvergent_16T": ScenarioSpec(
        "Precopy_Nonconvergent_16T", "migration", 16, "100% updates", 500_000
    ),
    "Postcopy_16T": ScenarioSpec("Postcopy_16T", "migration", 16, "100% updates", 500_000),
    "Postcopy_Late_16T_5s": ScenarioSpec(
        "Postcopy_Late_16T_5s", "migration", 16, "100% updates", 500_000
    ),
    "Postcopy_Late_16T_25s": ScenarioSpec(
        "Postcopy_Late_16T_25s", "migration", 16, "100% updates", 500_000
    ),
    "Postcopy_Late_16T_40s": ScenarioSpec(
        "Postcopy_Late_16T_40s", "migration", 16, "100% updates", 500_000
    ),
    "Autoconverge_16T": ScenarioSpec(
        "Autoconverge_16T", "migration", 16, "100% updates", 500_000
    ),
    "Precopy_Nonconvergent_OnlyReads_16T": ScenarioSpec(
        "Precopy_Nonconvergent_OnlyReads_16T", "migration", 16, "100% reads", None, True
    ),
    "Cold_Restart_4T": ScenarioSpec(
        "Cold_Restart_4T", "restart", 4, "100% updates", None, True
    ),
    "Prewarmed_4T": ScenarioSpec(
        "Prewarmed_4T", "restart", 4, "100% updates", None, True
    ),
    "Prepared_4T": ScenarioSpec(
        "Prepared_4T", "prepared", 4, "100% updates", None, True
    ),
}


METRIC_DEFINITIONS: dict[str, tuple[str, str]] = {
    "benchmark_runtime_host_s": ("Host-side interval from benchmark start to benchmark end.", "s"),
    "benchmark_runtime_ycsb_s": ("Maximum YCSB elapsed time in the workload CSV.", "s"),
    "steady_throughput_ops_s": (
        "Mean throughput in the scenario-specific baseline steady-state window.",
        "operations/s",
    ),
    "pre_throughput_ops_s": (
        f"Mean throughput during the {PRE_EVENT_WINDOW_S:g} s immediately before migration or relocation.",
        "operations/s",
    ),
    "during_throughput_ops_s": ("Mean throughput while migration is active.", "operations/s"),
    "pre_throttle_10s_throughput_ops_s": (
        "Mean throughput during the 10 s immediately before the first observed positive CPU throttle sample.",
        "operations/s",
    ),
    "throttled_throughput_ops_s": (
        "Mean throughput from the first observed positive CPU throttle sample until migration completion.",
        "operations/s",
    ),
    "post_0_10_throughput_ops_s": ("Mean throughput in the nominal (0, 10] s interval after completion/resumption.", "operations/s"),
    "post_10_30_throughput_ops_s": ("Mean throughput in the nominal (10, 30] s interval after completion/resumption.", "operations/s"),
    "post_30_90_throughput_ops_s": ("Mean of available throughput reports in the nominal (30, 90] s interval after completion/resumption.", "operations/s"),
    "post_30_90_observed_samples": ("Number of one-second YCSB reports available in the nominal (30, 90] s interval.", "reports"),
    "post_30_90_observed_duration_s": ("Duration represented by available one-second YCSB reports in the nominal (30, 90] s interval.", "s"),
    "post_30_90_complete": ("Whether all 60 expected one-second YCSB reports were available in the nominal (30, 90] s interval.", "boolean"),
    "migration_duration_event_s": ("Host event interval from migration_started to migration_end.", "s"),
    "migration_duration_qmp_s": ("QEMU query-migrate total-time converted from ms to s.", "s"),
    "migration_downtime_ms": ("QEMU query-migrate downtime.", "ms"),
    "service_interruption_s": ("Mechanism-specific host-side service interruption.", "s"),
    "total_transferred_gib": ("Total RAM migration data transferred.", "GiB"),
    "precopy_transferred_gib": ("RAM transferred during precopy.", "GiB"),
    "postcopy_transferred_gib": ("RAM transferred during postcopy.", "GiB"),
    "downtime_transferred_gib": ("RAM transferred while the VM was stopped.", "GiB"),
    "transfer_amplification": ("Total transferred RAM divided by configured guest RAM.", "x guest RAM"),
    "postcopy_page_requests": ("Final QEMU postcopy-requests counter.", "requests"),
    "postcopy_remote_fault_count": ("Sum of the destination QEMU postcopy-latency-dist bins.", "faults"),
    "postcopy_blocktime_ms": ("Destination QEMU postcopy-blocktime.", "ms"),
    "postcopy_latency_ms": ("Destination QEMU postcopy-latency converted from ns to ms; QEMU-reported zero is retained.", "ms"),
    "max_cpu_throttle_pct": ("Maximum positive cpu-throttle-percentage observed in query-migrate.", "%"),
    "throttle_start_after_migration_s": (
        "Time from migration start to the first observed positive CPU throttle sample.", "s"
    ),
    "throttle_active_observed_span_s": (
        "Time between the first and last observed positive CPU throttle samples.", "s"
    ),
    "throttle_active_until_migration_end_s": (
        "Time from the first observed positive throttle sample until migration completion.", "s"
    ),
    "max_throttle_first_after_migration_s": (
        "Time from migration start until the maximum throttle value was first observed.", "s"
    ),
    "max_throttle_observed_span_s": (
        "Time between the first and last samples at the maximum observed throttle value.", "s"
    ),
    "pre_buffer_hit_pct": ("Pooled PostgreSQL buffer hit ratio in the pre-event window.", "%"),
    "post_buffer_hit_pct": ("Pooled PostgreSQL buffer hit ratio in the late post-event window.", "%"),
    "observed_service_gap_s": ("Guest-wall-clock gap between the last source and first destination YCSB reports.", "s"),
    "destination_ready_to_benchmark_s": (
        "Interval from destination PostgreSQL readiness to destination workload start.", "s"
    ),
    "prepared_boot_throughput_ops_s": (
        "Prepared scenario source throughput from destination boot until standby PostgreSQL readiness.",
        "operations/s",
    ),
    "prepared_catchup_throughput_ops_s": (
        "Prepared scenario source throughput from standby PostgreSQL readiness until lag threshold.",
        "operations/s",
    ),
    "prepared_boot_phase_duration_s": (
        "Prepared scenario interval from destination boot to standby PostgreSQL readiness.", "s"
    ),
    "prepared_catchup_duration_s": (
        "Prepared scenario interval from standby PostgreSQL readiness to lag threshold.", "s"
    ),
    "prepared_lag_at_shutdown_mib": ("Replication lag reported when standby_ready was emitted.", "MiB"),
    "prepared_shutdown_duration_s": ("Source shutdown request to source powered off.", "s"),
    "prepared_replay_drain_s": ("Standby replay-drain start to replay drained.", "s"),
    "prepared_promotion_duration_s": ("Promotion start to promotion completion.", "s"),
    "prepared_handover_duration_s": (
        "Source shutdown request to destination benchmark start.", "s"
    ),
    "prepared_source_alone_disk_write_mib_s": (
        "Physical host disk writes during the final 20 s before destination boot.", "MiB/s"
    ),
    "prepared_boot_disk_write_mib_s": ("Physical host disk writes during destination boot.", "MiB/s"),
    "prepared_catchup_disk_write_mib_s": ("Physical host disk writes during standby catch-up.", "MiB/s"),
    "prepared_source_alone_io_full_avg10_pct": (
        "Host full I/O pressure avg10 during the final 20 s before destination boot.", "%"
    ),
    "prepared_boot_io_full_avg10_pct": ("Host full I/O pressure avg10 during destination boot.", "%"),
    "prepared_catchup_io_full_avg10_pct": ("Host full I/O pressure avg10 during standby catch-up.", "%"),
    "anon_hugepages_t0_pct_rss": ("AnonHugePages/RSS immediately after migration completion.", "%"),
    "anon_hugepages_t10_pct_rss": ("AnonHugePages/RSS 10 s after migration completion.", "%"),
    "anon_hugepages_t30_pct_rss": ("AnonHugePages/RSS 30 s after migration completion.", "%"),
}

# Metrics retained from the earlier builder, plus definitions for fields that
# v3 extracted without documenting. Canonical v3 names are used where an older
# field was only an alias for the same measurement.
METRIC_DEFINITIONS.update({
    "threads": ("Configured YCSB worker-thread count.", "threads"),
    "operation_count_configured": ("Configured YCSB operation count.", "operations"),
    "read_proportion": ("Configured YCSB read-operation proportion.", "proportion"),
    "update_proportion": ("Configured YCSB update-operation proportion.", "proportion"),
    "postcopy_delay_s": ("Configured delay before requesting post-copy.", "s"),
    "guest_memory_gib": ("Configured guest memory.", "GiB"),
    "ycsb_total_operations": ("Maximum cumulative YCSB operation count.", "operations"),
    "timed_workload": ("Whether the scenario uses a time limit instead of a fixed operation count.", "boolean"),
    "ycsb_operation_count_ok": ("True when a fixed operation count matched; timed workloads are exempt and recorded as true.", "boolean"),
    "source_ycsb_total_operations": ("Maximum cumulative source YCSB operation count.", "operations"),
    "destination_ycsb_total_operations": ("Maximum cumulative destination YCSB operation count.", "operations"),
    "steady_avg_latency_ms": ("Mean reported average latency in the fixed baseline steady-state window.", "ms"),
    "steady_p99_latency_ms": ("Mean reported p99 latency in the fixed baseline steady-state window.", "ms"),
    "steady_buffer_hit_pct": ("Pooled PostgreSQL buffer-hit ratio in the fixed baseline steady-state window.", "%"),
    "pre_avg_latency_ms": ("Mean reported average latency in the pre-event window.", "ms"),
    "pre_p99_latency_ms": ("Mean reported p99 latency in the pre-event window.", "ms"),
    "during_avg_latency_ms": ("Mean reported average latency while live migration is active.", "ms"),
    "during_p99_latency_ms": ("Mean reported p99 latency while live migration is active.", "ms"),
    "post_0_10_avg_latency_ms": ("Mean reported average latency during the first 10 execution seconds after the event.", "ms"),
    "post_0_10_p99_latency_ms": ("Mean reported p99 latency during the first 10 execution seconds after the event.", "ms"),
    "post_30_90_avg_latency_ms": ("Mean reported average latency from 30 to 90 execution seconds after the event.", "ms"),
    "post_30_90_p99_latency_ms": ("Mean reported p99 latency from 30 to 90 execution seconds after the event.", "ms"),
    "post_0_10_buffer_hit_pct": ("Pooled PostgreSQL buffer-hit ratio during the first 10 execution seconds after the event.", "%"),
    "migration_start_run_s": ("Migration-start event time relative to run start.", "s"),
    "migration_end_run_s": ("Migration-end event time relative to run start.", "s"),
    "migration_start_ycsb_s": ("Migration-start event time relative to benchmark start.", "s"),
    "migration_end_ycsb_s": ("Migration-end event time relative to benchmark start.", "s"),
    "migration_completed_before_benchmark_end": ("Whether migration completed no later than benchmark end.", "boolean"),
    "migration_end_minus_benchmark_end_s": ("Migration end minus benchmark end; positive values mean migration outlasted the workload.", "s"),
    "dirty_sync_count": ("Final QEMU dirty-sync-count.", "count"),
    "median_active_dirty_pages_s": ("Median positive QEMU dirty-pages-rate across active and postcopy-active samples.", "pages/s"),
    "median_active_dirty_mib_s": ("Median active dirty-pages-rate converted using the reported page size.", "MiB/s"),
    "active_dirty_sample_count": ("Number of positive dirty-pages-rate samples in active or postcopy-active QMP states.", "samples"),
    "median_active_transfer_mib_s": ("Median positive QEMU RAM transfer rate across active and postcopy-active samples, converted from Mbit/s to MiB/s.", "MiB/s"),
    "active_transfer_sample_count": ("Number of positive RAM-transfer-rate samples in active or postcopy-active QMP states.", "samples"),
    "qemu_completed_transfer_rate_mib_s": ("QEMU completed-state whole-migration RAM transfer rate converted from Mbit/s to MiB/s.", "MiB/s"),
    "stopcopy_operations_before_pause": ("Maximum cumulative YCSB operations before the stop-copy pause.", "operations"),
    "post_resume_host_throughput_ops_s": ("Host-time average throughput after stop-copy destination resumption.", "operations/s"),
    "destination_boot_to_postgres_s": ("Destination boot start to PostgreSQL readiness.", "s"),
    "destination_boot_to_ssh_s": ("Destination boot start to SSH readiness in the prepared scenario.", "s"),
    "source_shutdown_duration_s": ("Source shutdown request to source powered off.", "s"),
    "control_plane_service_gap_s": ("Source powerdown request to destination benchmark start.", "s"),
    "source_active_at_shutdown": ("Whether a source YCSB report occurred within 2.5 s before prepared shutdown.", "boolean"),
    "prepared_preparation_duration_s": ("Destination boot to prepared standby lag threshold.", "s"),
    "prepared_promotion_to_benchmark_s": ("Prepared promotion completion to destination benchmark start.", "s"),
})

for _offset in HUGEPAGE_SAMPLE_TIMES_S:
    _when = "at migration completion" if _offset == 0 else f"{_offset} s after migration completion"
    METRIC_DEFINITIONS.update({
        f"rss_t{_offset}_mib": (f"Process RSS {_when}.", "MiB"),
        f"anon_hugepages_t{_offset}_mib": (f"Anonymous huge-page memory {_when}.", "MiB"),
        f"anon_hugepages_t{_offset}_pct_rss": (f"AnonHugePages divided by RSS {_when}.", "%"),
        f"hugepages_t{_offset}_available": (f"Whether a huge-page sample was parsed {_when}.", "boolean"),
    })

# Remove v3's incorrect non-cumulative 30-second definition. The archived
# logger sleeps 0, 10, 30, 60, ... seconds successively, giving nominal
# cumulative sample times 0, 10, 40, 100, ... seconds. No archived run
# contains the later nominal 220-second sample.
METRIC_DEFINITIONS.pop("anon_hugepages_t30_pct_rss", None)

YCSB_INTERVAL_END_METRICS = {
    "steady_throughput_ops_s",
    "steady_avg_latency_ms",
    "steady_p99_latency_ms",
    "steady_buffer_hit_pct",
    "pre_throughput_ops_s",
    "pre_avg_latency_ms",
    "pre_p99_latency_ms",
    "during_throughput_ops_s",
    "pre_throttle_10s_throughput_ops_s",
    "throttled_throughput_ops_s",
    "during_avg_latency_ms",
    "during_p99_latency_ms",
    "post_0_10_throughput_ops_s",
    "post_0_10_avg_latency_ms",
    "post_0_10_p99_latency_ms",
    "post_0_10_buffer_hit_pct",
    "post_10_30_throughput_ops_s",
    "post_30_90_throughput_ops_s",
    "post_30_90_avg_latency_ms",
    "post_30_90_p99_latency_ms",
    "post_buffer_hit_pct",
    "prepared_boot_throughput_ops_s",
    "prepared_catchup_throughput_ops_s",
}
POST_30_90_METRICS = {
    "post_30_90_throughput_ops_s",
    "post_30_90_avg_latency_ms",
    "post_30_90_p99_latency_ms",
    "post_buffer_hit_pct",
    "post_30_90_observed_samples",
    "post_30_90_observed_duration_s",
    "post_30_90_complete",
}


def metric_method_note(scenario: str, metric: str) -> str:
    """Return concise design detail needed to reproduce a metric."""
    notes: list[str] = []
    if metric.startswith("steady_") and scenario in BASELINE_STEADY_WINDOWS_S:
        start_s, end_s = BASELINE_STEADY_WINDOWS_S[scenario]
        notes.append(
            f"Scenario window ({start_s:g}, {end_s:g}] s of YCSB elapsed time."
        )
    elif metric in YCSB_INTERVAL_END_METRICS:
        notes.append(
            "YCSB/pgstats rows are interval-end reports selected with "
            "elapsed_sec > start and elapsed_sec <= end."
        )

    if metric in POST_30_90_METRICS:
        notes.append(
            "The nominal 60-second window is retained even when the workload "
            "ends early; available reports are used and coverage is reported separately."
        )
    if metric in {"steady_buffer_hit_pct", "pre_buffer_hit_pct", "post_buffer_hit_pct", "post_0_10_buffer_hit_pct"}:
        notes.append("PostgreSQL hit/read deltas are pooled within each run before scenario aggregation.")
    if metric in {
        "median_active_dirty_pages_s",
        "median_active_dirty_mib_s",
        "active_dirty_sample_count",
        "median_active_transfer_mib_s",
        "active_transfer_sample_count",
    }:
        notes.append(
            "Only positive samples whose QMP migration status is active or "
            "postcopy-active are included; completed and device states are excluded."
        )
    if metric == "qemu_completed_transfer_rate_mib_s":
        notes.append(
            "Taken only from the completed QMP state carrying RAM counters; "
            "this whole-migration average is not mixed with active interval samples."
        )
    if metric == "postcopy_latency_ms":
        notes.append(
            "QEMU reports nanoseconds; values are divided by 1,000,000. "
            "A reported zero remains zero, including when no remote faults occurred."
        )
    if metric == "observed_service_gap_s":
        notes.append(
            "Calculated from timestamps emitted inside different guests and therefore "
            "assumes sufficiently synchronized guest wall clocks."
        )
    if metric.startswith(("rss_t", "anon_hugepages_t", "hugepages_t")):
        notes.append(
            "Offsets are nominal cumulative times reconstructed from the archived "
            "logger's successive sleep labels."
        )
    return " ".join(notes)


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def numeric(value: Any) -> float:
    """Convert a value to float, returning NaN for missing/non-numeric data."""
    result = pd.to_numeric(value, errors="coerce")
    return float(result) if pd.notna(result) else float("nan")


def finite(value: Any) -> bool:
    return math.isfinite(numeric(value))


def milliseconds_to_seconds(value: Any) -> float:
    number = numeric(value)
    return number / 1000.0 if math.isfinite(number) else float("nan")


def nanoseconds_to_milliseconds(value: Any) -> float:
    number = numeric(value)
    return number / 1_000_000.0 if math.isfinite(number) else float("nan")


def megabits_to_mebibytes_per_second(value: Any) -> float:
    number = numeric(value)
    return number * 1_000_000 / 8 / BYTES_PER_MIB if math.isfinite(number) else float("nan")


def safe_delta(end: Any, start: Any) -> float:
    end_value = numeric(end)
    start_value = numeric(start)
    if not (math.isfinite(end_value) and math.isfinite(start_value)):
        return float("nan")
    return end_value - start_value


def compute_input_fingerprint(root: Path) -> str:
    """Hash the paths and bytes of files that can contribute to extracted metrics."""
    selected: set[Path] = set()
    for scenario_name in SCENARIOS:
        directory = root / scenario_name
        if not directory.is_dir():
            continue
        for pattern in INPUT_FILE_PATTERNS:
            selected.update(path for path in directory.glob(pattern) if path.is_file())

    digest = hashlib.sha256()
    for path in sorted(selected, key=lambda item: item.relative_to(root).as_posix()):
        relative = path.relative_to(root).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()


def resolve_root(input_path: Path, temp_dir: Path) -> Path:
    """Return the directory containing scenario folders, extracting an archive if needed."""
    if input_path.is_file():
        if not tarfile.is_tarfile(input_path):
            raise ValueError(f"Unsupported input archive: {input_path}")
        with tarfile.open(input_path, "r:*") as archive:
            archive.extractall(temp_dir)
        candidates = [path for path in temp_dir.rglob("*") if path.is_dir()]
    elif input_path.is_dir():
        candidates = [input_path] + [path for path in input_path.rglob("*") if path.is_dir()]
    else:
        raise FileNotFoundError(input_path)

    required_names = set(SCENARIOS)
    scored: list[tuple[int, Path]] = []
    for candidate in candidates:
        child_dirs = {path.name for path in candidate.iterdir() if path.is_dir()}
        score = len(required_names & child_dirs)
        if score:
            scored.append((score, candidate))
    if not scored:
        raise ValueError("Could not find scenario directories in the supplied input.")
    return max(scored, key=lambda item: item[0])[1]


def parse_specs(directory: Path) -> dict[str, Any]:
    """Read the scenario specification file while preserving its original keys."""
    files = sorted(directory.glob("specs_*"))
    if not files:
        return {}
    result: dict[str, Any] = {}
    for line in files[0].read_text(encoding="utf-8", errors="replace").splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip().lower().replace(" ", "_")
        value = value.strip()
        try:
            result[key] = float(value) if "." in value else int(value)
        except ValueError:
            result[key] = value
    return result


def read_events(path: Path) -> tuple[pd.DataFrame, dict[str, list[dict[str, Any]]]]:
    frame = pd.read_csv(path)
    events: dict[str, list[dict[str, Any]]] = {}
    for row in frame.to_dict(orient="records"):
        events.setdefault(str(row.get("event", "")), []).append(row)
    return frame, events


def event_value(
    events: dict[str, list[dict[str, Any]]],
    *names: str,
    field: str = "t_since_run_start",
    last: bool = False,
    required: bool = False,
) -> float:
    for name in names:
        rows = events.get(name, [])
        if rows:
            value = rows[-1 if last else 0].get(field)
            number = numeric(value)
            if math.isfinite(number):
                return number
    if required:
        raise KeyError(f"Missing event {names}")
    return float("nan")


def event_details(events: dict[str, list[dict[str, Any]]], *names: str) -> str:
    for name in names:
        rows = events.get(name, [])
        if rows:
            return str(rows[0].get("details", ""))
    return ""


def read_ycsb(path: Path) -> tuple[pd.DataFrame, float]:
    frame = pd.read_csv(path)
    required = {"elapsed_sec", "total_operations", "current_ops_per_sec"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"{path} missing columns: {sorted(missing)}")

    for column in frame.columns:
        if column != "timestamp":
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    total_operations = float(frame["total_operations"].max())

    # YCSB may append a partial final sample with the same elapsed second.
    # Retain the first record for time-window rates, while preserving the maximum
    # operation count above for completion validation.
    frame = (
        frame.sort_values(["elapsed_sec"])
        .drop_duplicates(subset=["elapsed_sec"], keep="first")
        .reset_index(drop=True)
    )
    return frame, total_operations


def mean_window(frame: pd.DataFrame, column: str, start_s: float, end_s: float) -> float:
    if column not in frame.columns or not (math.isfinite(start_s) and math.isfinite(end_s)):
        return float("nan")
    values = frame.loc[
        (frame["elapsed_sec"] > start_s) & (frame["elapsed_sec"] <= end_s), column
    ].dropna()
    return float(values.mean()) if not values.empty else float("nan")


def add_window_coverage(
    row: dict[str, Any],
    frame: pd.DataFrame,
    prefix: str,
    start_s: float,
    end_s: float,
) -> None:
    """Record coverage for nominal one-second, interval-end YCSB windows."""
    if "elapsed_sec" not in frame.columns:
        row[f"{prefix}_observed_samples"] = float("nan")
        row[f"{prefix}_observed_duration_s"] = float("nan")
        row[f"{prefix}_complete"] = False
        return

    selected = frame.loc[
        (frame["elapsed_sec"] > start_s) & (frame["elapsed_sec"] <= end_s)
    ]
    observed_samples = int(selected["elapsed_sec"].nunique())
    expected_samples = max(
        0,
        int(math.floor(end_s + 1e-9) - math.floor(start_s + 1e-9)),
    )
    row[f"{prefix}_observed_samples"] = observed_samples
    row[f"{prefix}_observed_duration_s"] = float(observed_samples)
    row[f"{prefix}_complete"] = observed_samples >= expected_samples


def add_latency_window(
    row: dict[str, Any], frame: pd.DataFrame, prefix: str, start_s: float, end_s: float
) -> None:
    """Add average and p99 latency means for a YCSB execution-time window."""
    if "avg_latency_us" in frame.columns:
        row[f"{prefix}_avg_latency_ms"] = (
            mean_window(frame, "avg_latency_us", start_s, end_s) / 1000.0
        )
    if "p99_latency_us" in frame.columns:
        row[f"{prefix}_p99_latency_ms"] = (
            mean_window(frame, "p99_latency_us", start_s, end_s) / 1000.0
        )


def pooled_hit_ratio(frame: pd.DataFrame, start_s: float, end_s: float) -> float:
    required = {"elapsed_sec", "hit_delta", "read_delta"}
    if not required.issubset(frame.columns):
        return float("nan")
    selected = frame.loc[
        (frame["elapsed_sec"] > start_s) & (frame["elapsed_sec"] <= end_s)
    ]
    hits = pd.to_numeric(selected["hit_delta"], errors="coerce").sum(min_count=1)
    reads = pd.to_numeric(selected["read_delta"], errors="coerce").sum(min_count=1)
    denominator = hits + reads
    if pd.isna(denominator) or denominator <= 0:
        return float("nan")
    return float(100.0 * hits / denominator)


def read_pgstats(path: Path | None) -> pd.DataFrame | None:
    if path is None or not path.exists() or path.stat().st_size == 0:
        return None
    frame = pd.read_csv(path)
    for column in frame.columns:
        if column != "timestamp":
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def parse_ycsb_timestamp(value: Any) -> pd.Timestamp | None:
    if value is None or pd.isna(value):
        return None
    # YCSB uses yyyy-mm-dd HH:MM:SS:ms; replace final colon before milliseconds.
    text = str(value)
    match = re.match(r"^(.*\d{2}:\d{2}:\d{2}):(\d{3})$", text)
    if match:
        text = f"{match.group(1)}.{match.group(2)}"
    parsed = pd.to_datetime(text, errors="coerce")
    return None if pd.isna(parsed) else parsed


def observed_gap_seconds(source: pd.DataFrame, destination: pd.DataFrame) -> float:
    if "timestamp" not in source.columns or "timestamp" not in destination.columns:
        return float("nan")
    source_times = [parse_ycsb_timestamp(value) for value in source["timestamp"]]
    dest_times = [parse_ycsb_timestamp(value) for value in destination["timestamp"]]
    source_times = [value for value in source_times if value is not None]
    dest_times = [value for value in dest_times if value is not None]
    if not source_times or not dest_times:
        return float("nan")
    return float((min(dest_times) - max(source_times)).total_seconds())


def mean_host_window(frame: pd.DataFrame, column: str, start_s: float, end_s: float, divisor: float = 1.0) -> float:
    if column not in frame.columns or not (math.isfinite(start_s) and math.isfinite(end_s)):
        return float("nan")
    values = frame.loc[
        (frame["t_since_run_start"] >= start_s) & (frame["t_since_run_start"] < end_s), column
    ].dropna()
    return float(values.mean() / divisor) if not values.empty else float("nan")


def deep_merge(target: dict[str, Any], source: dict[str, Any]) -> None:
    for key, value in source.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            deep_merge(target[key], value)
        else:
            target[key] = value


# ---------------------------------------------------------------------------
# QMP migration parsing
# ---------------------------------------------------------------------------

def read_migration(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    records = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(records, list):
        raise ValueError(f"Expected a JSON list in {path}")

    states: list[dict[str, Any]] = []
    timed_states: list[tuple[float, dict[str, Any]]] = []
    completed_states: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        state = record.get("query_migrate", {})
        if not isinstance(state, dict) or not state:
            continue
        states.append(state)
        sample_time = numeric(record.get("t_since_run_start"))
        if math.isfinite(sample_time):
            timed_states.append((sample_time, state))
        if state.get("status") == "completed":
            completed_states.append(state)

    if not completed_states:
        last_status = timed_states[-1][1].get("status") if timed_states else None
        raise ValueError(f"Migration did not complete; last status={last_status}")

    merged: dict[str, Any] = {}
    for state in completed_states:
        # Work on copies so merging split completed records cannot mutate the
        # original samples used for per-sample medians below.
        deep_merge(merged, copy.deepcopy(state))

    # Prefer the completed state carrying RAM counters for final migration volume.
    ram_completed = next(
        (state for state in reversed(completed_states) if isinstance(state.get("ram"), dict) and state["ram"]),
        {},
    )
    if ram_completed:
        merged["ram"] = copy.deepcopy(ram_completed["ram"])
        for key in ("total-time", "downtime", "setup-time", "expected-downtime"):
            if key in ram_completed:
                merged[key] = ram_completed[key]

    all_ram = [state.get("ram", {}) for state in states if isinstance(state.get("ram"), dict)]
    active_ram = [
        state["ram"]
        for state in states
        if state.get("status") in ACTIVE_QMP_STATUSES
        and isinstance(state.get("ram"), dict)
    ]
    page_sizes = [numeric(ram.get("page-size")) for ram in all_ram]
    page_sizes = [value for value in page_sizes if math.isfinite(value) and value > 0]
    page_size = float(np.median(page_sizes)) if page_sizes else 4096.0

    dirty_rates = [numeric(ram.get("dirty-pages-rate")) for ram in active_ram]
    dirty_rates = [value for value in dirty_rates if math.isfinite(value) and value > 0]
    transfer_rates_mbps = [numeric(ram.get("mbps")) for ram in active_ram]
    transfer_rates_mbps = [
        value for value in transfer_rates_mbps if math.isfinite(value) and value > 0
    ]
    completed_transfer_mbps = (
        numeric(ram_completed.get("ram", {}).get("mbps")) if ram_completed else float("nan")
    )
    latency_distribution = merged.get("postcopy-latency-dist", [])
    remote_fault_count = (
        float(sum(numeric(value) for value in latency_distribution))
        if isinstance(latency_distribution, list)
        and latency_distribution
        and all(math.isfinite(numeric(value)) for value in latency_distribution)
        else float("nan")
    )
    throttle_samples: list[tuple[float, float]] = []
    statuses = [str(state.get("status")) for state in states if state.get("status") is not None]
    entered_postcopy = any(
        state.get("status") in {"postcopy-active", "postcopy-paused", "postcopy-recover"}
        for state in states
    )
    for sample_time, state in timed_states:
        throttle = numeric(state.get("cpu-throttle-percentage"))
        if math.isfinite(throttle) and throttle > 0:
            throttle_samples.append((sample_time, throttle))

    max_throttle = max((value for _, value in throttle_samples), default=float("nan"))
    max_times = [time for time, value in throttle_samples if value == max_throttle]

    derived = {
        "median_active_dirty_mib_s": (
            float(np.median(dirty_rates)) * page_size / BYTES_PER_MIB if dirty_rates else float("nan")
        ),
        "median_active_dirty_pages_s": (
            float(np.median(dirty_rates)) if dirty_rates else float("nan")
        ),
        "active_dirty_sample_count": len(dirty_rates),
        "median_active_transfer_mib_s": (
            megabits_to_mebibytes_per_second(float(np.median(transfer_rates_mbps)))
            if transfer_rates_mbps
            else float("nan")
        ),
        "active_transfer_sample_count": len(transfer_rates_mbps),
        "qemu_completed_transfer_rate_mib_s": megabits_to_mebibytes_per_second(
            completed_transfer_mbps
        ),
        "postcopy_remote_fault_count": remote_fault_count,
        "statuses": statuses,
        "entered_postcopy": entered_postcopy,
        "max_cpu_throttle_pct": max_throttle,
        "throttle_first_run_s": throttle_samples[0][0] if throttle_samples else float("nan"),
        "throttle_last_run_s": throttle_samples[-1][0] if throttle_samples else float("nan"),
        "throttle_observed_span_s": (
            throttle_samples[-1][0] - throttle_samples[0][0]
            if len(throttle_samples) >= 2
            else (0.0 if len(throttle_samples) == 1 else float("nan"))
        ),
        "max_throttle_first_run_s": min(max_times) if max_times else float("nan"),
        "max_throttle_observed_span_s": (
            max(max_times) - min(max_times)
            if len(max_times) >= 2
            else (0.0 if len(max_times) == 1 else float("nan"))
        ),
    }
    return merged, derived


def qmp_ram_value(state: dict[str, Any], key: str) -> float:
    ram = state.get("ram", {}) if isinstance(state.get("ram"), dict) else {}
    return numeric(ram.get(key))


# ---------------------------------------------------------------------------
# Huge-page parsing
# ---------------------------------------------------------------------------

def read_hugepages(path: Path | None) -> dict[int, dict[str, float]]:
    """Parse huge-page samples and convert logged sleep delays to elapsed time.

    The logger writes lines such as ``30: Rss: ...`` after sleeping for each
    successive label. Therefore labels 0, 10, 30, 60 correspond to actual
    elapsed offsets 0, 10, 40, 100 seconds.
    """
    if path is None or not path.exists() or path.stat().st_size == 0:
        return {}
    text = path.read_text(errors="replace")
    result: dict[int, dict[str, float]] = {}
    # The first metric can occur on the same line as the delay label.
    matches = list(re.finditer(r"(?m)^(\d+):[ \t]*", text))
    elapsed_s = 0
    for index, match in enumerate(matches):
        elapsed_s += int(match.group(1))
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        block = text[match.end():end]
        rss_match = re.search(r"(?m)^Rss:\s+(\d+)\s+kB", block)
        huge_match = re.search(r"(?m)^AnonHugePages:\s+(\d+)\s+kB", block)
        if rss_match and huge_match:
            rss_kib = float(rss_match.group(1))
            huge_kib = float(huge_match.group(1))
            result[elapsed_s] = {
                "rss_mib": rss_kib / 1024.0,
                "anon_hugepages_mib": huge_kib / 1024.0,
                "anon_hugepages_pct_rss": (
                    100.0 * huge_kib / rss_kib if rss_kib > 0 else float("nan")
                ),
            }
    return result


# ---------------------------------------------------------------------------
# Scenario extractors
# ---------------------------------------------------------------------------

def common_row(spec: ScenarioSpec, directory: Path, run: int) -> dict[str, Any]:
    specs = parse_specs(directory)
    return {
        "scenario": spec.name,
        "run": run,
        "kind": spec.kind,
        "threads": spec.threads,
        "workload": spec.workload,
        "timed_workload": spec.timed_workload,
        "operation_count_configured": numeric(specs.get("opcount")),
        "read_proportion": numeric(specs.get("read")),
        "update_proportion": numeric(specs.get("update-prop")),
        "postcopy_delay_s": numeric(specs.get("postcopy-sleep_after_mig_start")),
        "guest_memory_gib": numeric(specs.get("memory", GUEST_RAM_GIB)),
        "valid_primary": True,
        "exclusion_reason": "",
        "ycsb_operation_count_ok": True if spec.timed_workload else None,
    }


def extract_baseline(spec: ScenarioSpec, directory: Path, run: int) -> dict[str, Any]:
    row = common_row(spec, directory, run)
    events_frame, events = read_events(directory / f"events-run-{run}.csv")
    bench, operations = read_ycsb(directory / f"bench-run-{run}-final.csv")
    steady_start_s, steady_end_s = BASELINE_STEADY_WINDOWS_S[spec.name]
    benchmark_start = event_value(events, "benchmark_start", "source_benchmark_start", required=True)
    benchmark_end = event_value(events, "benchmark_end", required=True)
    row.update({
        "ycsb_total_operations": operations,
        "ycsb_operation_count_ok": (
            operations == spec.expected_operations if spec.expected_operations is not None else True
        ),
        "benchmark_runtime_host_s": benchmark_end - benchmark_start,
        "benchmark_runtime_ycsb_s": float(bench["elapsed_sec"].max()),
        "steady_throughput_ops_s": mean_window(
            bench, "current_ops_per_sec", steady_start_s, steady_end_s
        ),
    })
    add_latency_window(row, bench, "steady", steady_start_s, steady_end_s)
    pg_path = directory / f"bench-run-{run}-final-pgstats.csv"
    row["pgstats_available"] = pg_path.exists() and pg_path.stat().st_size > 0
    if row["pgstats_available"]:
        pg = read_pgstats(pg_path)
        if pg is not None:
            row["steady_buffer_hit_pct"] = pooled_hit_ratio(
                pg, steady_start_s, steady_end_s
            )
    row["postgres_log_available"] = (directory / f"postgres-run-{run}-final.json").exists()
    return row


def extract_migration(spec: ScenarioSpec, directory: Path, run: int) -> dict[str, Any]:
    row = common_row(spec, directory, run)
    _, events = read_events(directory / f"events-run-{run}.csv")
    bench, operations = read_ycsb(directory / f"bench-run-{run}.csv")
    benchmark_start = event_value(events, "benchmark_start", required=True)
    benchmark_end = event_value(events, "benchmark_end", required=True)
    migration_start_run = event_value(events, "migration_started", required=True)
    migration_end_run = event_value(events, "migration_end", required=True)
    migration_start = migration_start_run - benchmark_start
    migration_end = migration_end_run - benchmark_start

    state, derived = read_migration(directory / f"mig-stats-run-{run}.json")
    total_bytes = qmp_ram_value(state, "transferred")
    precopy_bytes = qmp_ram_value(state, "precopy-bytes")
    postcopy_bytes = qmp_ram_value(state, "postcopy-bytes")
    downtime_bytes = qmp_ram_value(state, "downtime-bytes")

    first_throttle = numeric(derived.get("throttle_first_run_s"))
    throttle_start_ycsb = (
        first_throttle - benchmark_start
        if math.isfinite(first_throttle)
        else float("nan")
    )
    max_throttle_first = numeric(derived.get("max_throttle_first_run_s"))

    row.update({
        "ycsb_total_operations": operations,
        "ycsb_operation_count_ok": (
            operations == spec.expected_operations if spec.expected_operations is not None else True
        ),
        "benchmark_runtime_host_s": benchmark_end - benchmark_start,
        "benchmark_runtime_ycsb_s": float(bench["elapsed_sec"].max()),
        "migration_completed": True,
        "migration_status": str(state.get("status", "completed")),
        "migration_start_run_s": migration_start_run,
        "migration_end_run_s": migration_end_run,
        "migration_start_ycsb_s": migration_start,
        "migration_end_ycsb_s": migration_end,
        "migration_duration_event_s": migration_end_run - migration_start_run,
        "migration_duration_qmp_s": milliseconds_to_seconds(state.get("total-time")),
        "migration_downtime_ms": numeric(state.get("downtime")),
        "pre_throughput_ops_s": mean_window(
            bench, "current_ops_per_sec", migration_start - PRE_EVENT_WINDOW_S, migration_start
        ),
        "during_throughput_ops_s": mean_window(
            bench, "current_ops_per_sec", migration_start, migration_end
        ),
        "pre_throttle_10s_throughput_ops_s": (
            mean_window(
                bench,
                "current_ops_per_sec",
                throttle_start_ycsb - PRE_THROTTLE_WINDOW_S,
                throttle_start_ycsb,
            )
            if math.isfinite(throttle_start_ycsb)
            else float("nan")
        ),
        "throttled_throughput_ops_s": (
            mean_window(
                bench,
                "current_ops_per_sec",
                throttle_start_ycsb,
                migration_end,
            )
            if math.isfinite(throttle_start_ycsb)
            else float("nan")
        ),
        "post_0_10_throughput_ops_s": mean_window(
            bench, "current_ops_per_sec", migration_end, migration_end + POST_EARLY_END_S
        ),
        "post_10_30_throughput_ops_s": mean_window(
            bench, "current_ops_per_sec", migration_end + POST_MID_START_S, migration_end + POST_MID_END_S
        ),
        "post_30_90_throughput_ops_s": mean_window(
            bench, "current_ops_per_sec", migration_end + POST_LATE_START_S, migration_end + POST_LATE_END_S
        ),
        "total_transferred_gib": total_bytes / BYTES_PER_GIB if math.isfinite(total_bytes) else float("nan"),
        "precopy_transferred_gib": precopy_bytes / BYTES_PER_GIB if math.isfinite(precopy_bytes) else float("nan"),
        "postcopy_transferred_gib": postcopy_bytes / BYTES_PER_GIB if math.isfinite(postcopy_bytes) else float("nan"),
        "downtime_transferred_gib": downtime_bytes / BYTES_PER_GIB if math.isfinite(downtime_bytes) else float("nan"),
        "transfer_amplification": (
            total_bytes / BYTES_PER_GIB / GUEST_RAM_GIB if math.isfinite(total_bytes) else float("nan")
        ),
        "median_active_dirty_pages_s": numeric(derived.get("median_active_dirty_pages_s")),
        "median_active_dirty_mib_s": numeric(derived.get("median_active_dirty_mib_s")),
        "active_dirty_sample_count": numeric(derived.get("active_dirty_sample_count")),
        "median_active_transfer_mib_s": numeric(derived.get("median_active_transfer_mib_s")),
        "active_transfer_sample_count": numeric(derived.get("active_transfer_sample_count")),
        "qemu_completed_transfer_rate_mib_s": numeric(
            derived.get("qemu_completed_transfer_rate_mib_s")
        ),
        "dirty_sync_count": qmp_ram_value(state, "dirty-sync-count"),
        "postcopy_page_requests": qmp_ram_value(state, "postcopy-requests"),
        "postcopy_remote_fault_count": numeric(derived.get("postcopy_remote_fault_count")),
        "postcopy_blocktime_ms": numeric(state.get("postcopy-blocktime")),
        "postcopy_latency_ms": nanoseconds_to_milliseconds(state.get("postcopy-latency")),
        "entered_postcopy": bool(derived.get("entered_postcopy", False)),
        "max_cpu_throttle_pct": numeric(derived.get("max_cpu_throttle_pct")),
        "throttle_start_after_migration_s": (
            first_throttle - migration_start_run if math.isfinite(first_throttle) else float("nan")
        ),
        "throttle_active_observed_span_s": numeric(derived.get("throttle_observed_span_s")),
        "throttle_active_until_migration_end_s": (
            migration_end_run - first_throttle if math.isfinite(first_throttle) else float("nan")
        ),
        "max_throttle_first_after_migration_s": (
            max_throttle_first - migration_start_run
            if math.isfinite(max_throttle_first)
            else float("nan")
        ),
        "max_throttle_observed_span_s": numeric(derived.get("max_throttle_observed_span_s")),
        "migration_end_minus_benchmark_end_s": migration_end_run - benchmark_end,
        "migration_completed_before_benchmark_end": migration_end_run <= benchmark_end,
    })

    add_latency_window(
        row, bench, "pre", migration_start - PRE_EVENT_WINDOW_S, migration_start
    )
    if spec.kind != "stopcopy":
        add_latency_window(row, bench, "during", migration_start, migration_end)
        add_latency_window(
            row, bench, "post_0_10", migration_end, migration_end + POST_EARLY_END_S
        )
        add_latency_window(
            row,
            bench,
            "post_30_90",
            migration_end + POST_LATE_START_S,
            migration_end + POST_LATE_END_S,
        )
        add_window_coverage(
            row,
            bench,
            "post_30_90",
            migration_end + POST_LATE_START_S,
            migration_end + POST_LATE_END_S,
        )

    pg_path = directory / f"bench-run-{run}-pgstats.csv"
    pg = read_pgstats(pg_path if pg_path.exists() else None)
    row["pgstats_available"] = pg is not None
    if pg is not None:
        row["pre_buffer_hit_pct"] = pooled_hit_ratio(
            pg, migration_start - PRE_EVENT_WINDOW_S, migration_start
        )
        if spec.kind != "stopcopy":
            row["post_0_10_buffer_hit_pct"] = pooled_hit_ratio(
                pg, migration_end, migration_end + POST_EARLY_END_S
            )
            row["post_buffer_hit_pct"] = pooled_hit_ratio(
                pg, migration_end + POST_LATE_START_S, migration_end + POST_LATE_END_S
            )

    huge_path = directory / f"hugepages-{run}.log"
    huge = read_hugepages(huge_path if huge_path.exists() else None)
    hugepages_expected = spec.name in HUGEPAGE_SCENARIOS
    row["hugepages_available"] = bool(huge) if hugepages_expected else None
    for offset in HUGEPAGE_SAMPLE_TIMES_S:
        row[f"hugepages_t{offset}_available"] = (
            offset in huge if hugepages_expected else None
        )
    for offset, sample in huge.items():
        row[f"rss_t{offset}_mib"] = sample["rss_mib"]
        row[f"anon_hugepages_t{offset}_mib"] = sample["anon_hugepages_mib"]
        row[f"anon_hugepages_t{offset}_pct_rss"] = sample["anon_hugepages_pct_rss"]

    row["postgres_log_available"] = (
        (directory / f"postgres-run-{run}.json").exists()
        and (directory / f"postgres-run-{run}.json").stat().st_size > 0
    )
    return row


def extract_stopcopy(spec: ScenarioSpec, directory: Path, run: int) -> dict[str, Any]:
    row = extract_migration(spec, directory, run)
    _, events = read_events(directory / f"events-run-{run}.csv")
    source_stopped = event_value(events, "source_stopped", required=True)
    destination_resumed = event_value(events, "destination_resumed", required=True)
    benchmark_end = event_value(events, "benchmark_end", required=True)
    bench, operations = read_ycsb(directory / f"bench-run-{run}.csv")

    # YCSB elapsed time does not represent host wall time across suspension.
    # Preserve the pre-event metric, but replace post-event summaries with a
    # host-side average based on operations remaining after the final pre-stop sample.
    benchmark_start = event_value(events, "benchmark_start", required=True)
    stop_ycsb = source_stopped - benchmark_start
    pre_rows = bench.loc[bench["elapsed_sec"] <= stop_ycsb]
    operations_before_stop = float(pre_rows["total_operations"].max()) if not pre_rows.empty else float("nan")
    remaining = operations - operations_before_stop if math.isfinite(operations_before_stop) else float("nan")
    host_post_duration = benchmark_end - destination_resumed
    post_host_throughput = (
        remaining / host_post_duration
        if math.isfinite(remaining) and host_post_duration > 0
        else float("nan")
    )

    row["service_interruption_s"] = destination_resumed - source_stopped
    row["stopcopy_operations_before_pause"] = operations_before_stop
    row["post_resume_host_throughput_ops_s"] = post_host_throughput
    row["during_throughput_ops_s"] = float("nan")
    row["post_0_10_throughput_ops_s"] = float("nan")
    row["post_10_30_throughput_ops_s"] = float("nan")
    row["post_30_90_throughput_ops_s"] = float("nan")
    row["post_30_90_observed_samples"] = float("nan")
    row["post_30_90_observed_duration_s"] = float("nan")
    row["post_30_90_complete"] = None
    row["post_0_10_buffer_hit_pct"] = float("nan")
    row["post_buffer_hit_pct"] = float("nan")
    return row


def extract_restart(spec: ScenarioSpec, directory: Path, run: int) -> dict[str, Any]:
    row = common_row(spec, directory, run)
    _, events = read_events(directory / f"events-run-{run}.csv")
    source, source_ops = read_ycsb(directory / f"bench-run-{run}-preshut.csv")
    destination, destination_ops = read_ycsb(directory / f"bench-run-{run}-final.csv")

    source_benchmark_start = event_value(events, "source_benchmark_start", "benchmark_start", required=True)
    source_powerdown = event_value(events, "source_powerdown_requested", required=True)
    source_powered_off = event_value(events, "source_powered_off", required=True)
    destination_boot = event_value(events, "destination_boot_start", required=True)
    destination_ready = event_value(events, "destination_postgres_ready", required=True)
    destination_benchmark = event_value(events, "destination_benchmark_start", required=True)
    benchmark_end = event_value(events, "benchmark_end", required=True)

    shutdown_ycsb = source_powerdown - source_benchmark_start
    row.update({
        "source_ycsb_total_operations": source_ops,
        "destination_ycsb_total_operations": destination_ops,
        "pre_throughput_ops_s": mean_window(
            source, "current_ops_per_sec", shutdown_ycsb - PRE_EVENT_WINDOW_S, shutdown_ycsb
        ),
        "post_0_10_throughput_ops_s": mean_window(
            destination, "current_ops_per_sec", 0.0, 10.0
        ),
        "post_10_30_throughput_ops_s": mean_window(
            destination, "current_ops_per_sec", 10.0, 30.0
        ),
        "post_30_90_throughput_ops_s": mean_window(
            destination, "current_ops_per_sec", 30.0, 90.0
        ),
        "source_shutdown_duration_s": source_powered_off - source_powerdown,
        "destination_boot_to_postgres_s": destination_ready - destination_boot,
        "destination_ready_to_benchmark_s": destination_benchmark - destination_ready,
        "control_plane_service_gap_s": destination_benchmark - source_powerdown,
        "observed_service_gap_s": observed_gap_seconds(source, destination),
        "benchmark_runtime_host_s": benchmark_end - source_benchmark_start,
    })

    add_latency_window(
        row, source, "pre", shutdown_ycsb - PRE_EVENT_WINDOW_S, shutdown_ycsb
    )
    add_latency_window(row, destination, "post_0_10", 0.0, POST_EARLY_END_S)
    add_latency_window(
        row, destination, "post_30_90", POST_LATE_START_S, POST_LATE_END_S
    )
    add_window_coverage(
        row, destination, "post_30_90", POST_LATE_START_S, POST_LATE_END_S
    )

    source_pg = read_pgstats(directory / f"bench-run-{run}-preshut-pgstats.csv")
    destination_pg = read_pgstats(directory / f"bench-run-{run}-final-pgstats.csv")
    row["pgstats_available"] = source_pg is not None and destination_pg is not None
    if source_pg is not None:
        row["pre_buffer_hit_pct"] = pooled_hit_ratio(
            source_pg, shutdown_ycsb - PRE_EVENT_WINDOW_S, shutdown_ycsb
        )
    if destination_pg is not None:
        row["post_0_10_buffer_hit_pct"] = pooled_hit_ratio(destination_pg, 0.0, 10.0)
        row["post_buffer_hit_pct"] = pooled_hit_ratio(destination_pg, 30.0, 90.0)

    row["postgres_log_available"] = True
    return row


def extract_prepared(spec: ScenarioSpec, directory: Path, run: int) -> dict[str, Any]:
    row = common_row(spec, directory, run)
    _, events = read_events(directory / f"events-run-{run}.csv")
    source, source_ops = read_ycsb(directory / f"bench-run-{run}-preshut.csv")
    destination, destination_ops = read_ycsb(directory / f"bench-run-{run}-final.csv")

    benchmark_start = event_value(events, "benchmark_start", "source_benchmark_start", required=True)
    destination_boot = event_value(events, "destination_boot_start", required=True)
    destination_ssh = event_value(events, "destination_ssh_ready")
    standby_postgres = event_value(events, "destination_standby_postgres_ready", required=True)
    standby_ready = event_value(events, "standby_ready", required=True)
    source_shutdown = event_value(events, "source_shutdown_request", required=True)
    source_powered_off = event_value(events, "source_powered_off", required=True)
    replay_start = event_value(events, "standby_replay_drain_start", required=True)
    replay_drained = event_value(events, "standby_replay_drained", required=True)
    promotion_start = event_value(events, "promotion_start", required=True)
    promotion_complete = event_value(events, "promotion_complete", required=True)
    destination_benchmark = event_value(events, "destination_benchmark_start", required=True)
    benchmark_end = event_value(events, "benchmark_end", required=True)

    boot_ycsb = destination_boot - benchmark_start
    standby_pg_ycsb = standby_postgres - benchmark_start
    standby_ready_ycsb = standby_ready - benchmark_start
    shutdown_ycsb = source_shutdown - benchmark_start

    source_actual_end_run = benchmark_start + float(source["elapsed_sec"].max())
    source_active_at_shutdown = source_actual_end_run >= source_shutdown - 2.5

    details = event_details(events, "standby_ready")
    lag_match = re.search(r"lag_bytes=(\d+)", details)
    lag_bytes = float(lag_match.group(1)) if lag_match else float("nan")

    row.update({
        "source_ycsb_total_operations": source_ops,
        "destination_ycsb_total_operations": destination_ops,
        "pre_throughput_ops_s": mean_window(
            source, "current_ops_per_sec", boot_ycsb - PREPARED_SOURCE_ALONE_WINDOW_S, boot_ycsb
        ),
        "prepared_boot_throughput_ops_s": mean_window(
            source, "current_ops_per_sec", boot_ycsb, standby_pg_ycsb
        ),
        "prepared_catchup_throughput_ops_s": mean_window(
            source, "current_ops_per_sec", standby_pg_ycsb, standby_ready_ycsb
        ),
        "prepared_boot_phase_duration_s": standby_postgres - destination_boot,
        "prepared_catchup_duration_s": standby_ready - standby_postgres,
        "prepared_preparation_duration_s": standby_ready - destination_boot,
        "prepared_lag_at_shutdown_mib": lag_bytes / BYTES_PER_MIB if math.isfinite(lag_bytes) else float("nan"),
        "prepared_shutdown_duration_s": source_powered_off - source_shutdown,
        "prepared_replay_drain_s": replay_drained - replay_start,
        "prepared_promotion_duration_s": promotion_complete - promotion_start,
        "prepared_promotion_to_benchmark_s": destination_benchmark - promotion_complete,
        "prepared_handover_duration_s": destination_benchmark - source_shutdown,
        "destination_boot_to_ssh_s": (
            destination_ssh - destination_boot if math.isfinite(destination_ssh) else float("nan")
        ),
        "source_active_at_shutdown": source_active_at_shutdown,
        "observed_service_gap_s": observed_gap_seconds(source, destination),
        "post_0_10_throughput_ops_s": mean_window(destination, "current_ops_per_sec", 0.0, 10.0),
        "post_10_30_throughput_ops_s": mean_window(destination, "current_ops_per_sec", 10.0, 30.0),
        "post_30_90_throughput_ops_s": mean_window(destination, "current_ops_per_sec", 30.0, 90.0),
        "benchmark_runtime_host_s": benchmark_end - benchmark_start,
    })

    add_latency_window(
        row,
        source,
        "pre",
        boot_ycsb - PREPARED_SOURCE_ALONE_WINDOW_S,
        boot_ycsb,
    )
    add_latency_window(row, destination, "post_0_10", 0.0, POST_EARLY_END_S)
    add_latency_window(
        row, destination, "post_30_90", POST_LATE_START_S, POST_LATE_END_S
    )
    add_window_coverage(
        row, destination, "post_30_90", POST_LATE_START_S, POST_LATE_END_S
    )

    host_path = directory / f"host-stats-run-{run}.csv"
    if host_path.exists() and host_path.stat().st_size > 0:
        host = pd.read_csv(host_path)
        for column in host.columns:
            host[column] = pd.to_numeric(host[column], errors="coerce")
        source_alone_start = max(benchmark_start, destination_boot - PREPARED_SOURCE_ALONE_WINDOW_S)
        row.update({
            "prepared_source_alone_disk_write_mib_s": mean_host_window(
                host, "disk_write_bytes_per_s", source_alone_start, destination_boot, BYTES_PER_MIB
            ),
            "prepared_boot_disk_write_mib_s": mean_host_window(
                host, "disk_write_bytes_per_s", destination_boot, standby_postgres, BYTES_PER_MIB
            ),
            "prepared_catchup_disk_write_mib_s": mean_host_window(
                host, "disk_write_bytes_per_s", standby_postgres, standby_ready, BYTES_PER_MIB
            ),
            "prepared_source_alone_io_full_avg10_pct": mean_host_window(
                host, "psi_io_full_avg10", source_alone_start, destination_boot
            ),
            "prepared_boot_io_full_avg10_pct": mean_host_window(
                host, "psi_io_full_avg10", destination_boot, standby_postgres
            ),
            "prepared_catchup_io_full_avg10_pct": mean_host_window(
                host, "psi_io_full_avg10", standby_postgres, standby_ready
            ),
        })

    source_pg = read_pgstats(directory / f"bench-run-{run}-preshut-pgstats.csv")
    destination_pg = read_pgstats(directory / f"bench-run-{run}-final-pgstats.csv")
    row["pgstats_available"] = source_pg is not None and destination_pg is not None
    if source_pg is not None:
        row["pre_buffer_hit_pct"] = pooled_hit_ratio(
            source_pg, boot_ycsb - PREPARED_SOURCE_ALONE_WINDOW_S, boot_ycsb
        )
    if destination_pg is not None:
        row["post_0_10_buffer_hit_pct"] = pooled_hit_ratio(destination_pg, 0.0, 10.0)
        row["post_buffer_hit_pct"] = pooled_hit_ratio(destination_pg, 30.0, 90.0)

    row["postgres_log_available"] = True
    return row


# ---------------------------------------------------------------------------
# Validation and aggregation
# ---------------------------------------------------------------------------

def exclude_primary(row: dict[str, Any], reason: str) -> None:
    row["valid_primary"] = False
    existing = str(row.get("exclusion_reason", "")).strip()
    row["exclusion_reason"] = f"{existing}; {reason}" if existing else reason


def validate_primary(spec: ScenarioSpec, row: dict[str, Any]) -> None:
    """Apply scenario-policy checks after successful extraction."""
    if spec.expected_operations is not None and row.get("ycsb_operation_count_ok") != True:  # noqa: E712
        exclude_primary(
            row,
            "YCSB did not reach the configured fixed operation count "
            f"({spec.expected_operations}).",
        )
    if spec.name in POSTCOPY_SCENARIOS and row.get("entered_postcopy") != True:  # noqa: E712
        exclude_primary(row, "Scenario was configured for postcopy but never entered postcopy.")
    if spec.kind == "prepared" and row.get("source_active_at_shutdown") != True:  # noqa: E712
        exclude_primary(
            row,
            "Source workload had no YCSB report within 2.5 s before prepared shutdown.",
        )


def process_run(spec: ScenarioSpec, directory: Path, run: int) -> dict[str, Any]:
    try:
        if spec.kind == "baseline":
            row = extract_baseline(spec, directory, run)
        elif spec.kind == "migration":
            row = extract_migration(spec, directory, run)
        elif spec.kind == "stopcopy":
            row = extract_stopcopy(spec, directory, run)
        elif spec.kind == "restart":
            row = extract_restart(spec, directory, run)
        elif spec.kind == "prepared":
            row = extract_prepared(spec, directory, run)
        else:
            raise ValueError(f"Unsupported scenario kind: {spec.kind}")
        validate_primary(spec, row)
    except Exception as error:  # Deliberately retain failed rows in the quality table.
        row = common_row(spec, directory, run)
        row["valid_primary"] = False
        row["exclusion_reason"] = f"Extraction failed: {type(error).__name__}: {error}"
    return row


def aggregate_runs(runs: pd.DataFrame, input_fingerprint: str) -> pd.DataFrame:
    valid = runs.loc[runs["valid_primary"] == True].copy()  # noqa: E712
    excluded = {
        "run",
        "valid_primary",
        "timed_workload",
        "ycsb_operation_count_ok",
        "migration_completed",
        "entered_postcopy",
        "pgstats_available",
        "postgres_log_available",
        "hugepages_available",
        "post_30_90_complete",
        "migration_completed_before_benchmark_end",
        "source_active_at_shutdown",
    }
    excluded.update(
        f"hugepages_t{offset}_available" for offset in HUGEPAGE_SAMPLE_TIMES_S
    )
    numeric_columns = [
        str(column)
        for column in valid.select_dtypes(include=[np.number]).columns
        if str(column) not in excluded
    ]

    output: list[dict[str, Any]] = []
    for scenario_key, group in valid.groupby("scenario", sort=False):
        scenario = str(scenario_key)
        for metric in numeric_columns:
            values = pd.to_numeric(group[metric], errors="coerce").dropna()
            if values.empty:
                continue
            definition, unit = METRIC_DEFINITIONS.get(metric, ("", ""))
            output.append({
                "scenario": scenario,
                "metric": metric,
                "definition": definition,
                "unit": unit,
                "n": int(values.count()),
                "mean": float(values.mean()),
                "std": float(values.std(ddof=1)) if len(values) > 1 else float("nan"),
                "median": float(values.median()),
                "q1": float(values.quantile(0.25)),
                "q3": float(values.quantile(0.75)),
                "min": float(values.min()),
                "max": float(values.max()),
                "method_note": metric_method_note(scenario, metric),
                "builder_version": VERSION,
                "input_fingerprint_sha256": input_fingerprint,
                "statistical_unit": "experimental run",
                "aggregation_method": AGGREGATION_METHOD,
            })
    return pd.DataFrame(output)


def write_outputs(runs: pd.DataFrame, output_dir: Path, input_fingerprint: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    runs = runs.copy()
    runs["builder_version"] = VERSION
    runs["input_fingerprint_sha256"] = input_fingerprint
    summary = aggregate_runs(runs, input_fingerprint)

    quality_columns = [
        column
        for column in [
            "scenario", "run", "valid_primary", "exclusion_reason",
            "timed_workload", "ycsb_operation_count_ok",
            "migration_completed", "entered_postcopy",
            "migration_completed_before_benchmark_end",
            "migration_end_minus_benchmark_end_s",
            "source_active_at_shutdown",
            "post_30_90_observed_samples", "post_30_90_observed_duration_s",
            "post_30_90_complete",
            "pgstats_available", "postgres_log_available", "hugepages_available",
            *[
                f"hugepages_t{offset}_available"
                for offset in HUGEPAGE_SAMPLE_TIMES_S
            ],
            "builder_version", "input_fingerprint_sha256",
        ]
        if column in runs.columns
    ]
    quality = runs[quality_columns].copy()

    definitions = pd.DataFrame([
        {
            "metric": metric,
            "definition": definition,
            "method_note": metric_method_note("", metric),
            "unit": unit,
            "builder_version": VERSION,
            "input_fingerprint_sha256": input_fingerprint,
        }
        for metric, (definition, unit) in METRIC_DEFINITIONS.items()
    ])

    runs.to_csv(output_dir / "evaluation_runs.csv", index=False)
    summary.to_csv(output_dir / "evaluation_summary.csv", index=False)
    quality.to_csv(output_dir / "data_quality.csv", index=False)
    definitions.to_csv(output_dir / "metric_definitions.csv", index=False)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, nargs="?", help="Extracted log root or .tar/.tar.xz archive")
    parser.add_argument("--output", type=Path, default=Path("evaluation_tables_final"))
    parser.add_argument("--version", action="store_true", help="Print script version and exit")
    args = parser.parse_args()

    if args.version:
        print(VERSION)
        return 0
    if args.input is None:
        parser.error("input is required unless --version is used")

    with tempfile.TemporaryDirectory(prefix="evaluation_logs_") as temporary:
        root = resolve_root(args.input.resolve(), Path(temporary))
        input_fingerprint = compute_input_fingerprint(root)
        rows: list[dict[str, Any]] = []
        for scenario_name, spec in SCENARIOS.items():
            directory = root / scenario_name
            if not directory.exists():
                print(f"WARNING: missing scenario directory: {directory}")
                continue
            for run in range(1, 11):
                print(f"Processing {scenario_name} run {run}")
                rows.append(process_run(spec, directory, run))

        runs = pd.DataFrame(rows)
        write_outputs(runs, args.output.resolve(), input_fingerprint)

    valid_count = int(runs["valid_primary"].fillna(False).sum())
    print(f"\nVersion: {VERSION}")
    print(f"Processed: {len(runs)} runs")
    print(f"Valid for primary aggregation: {valid_count}")
    print(f"Excluded: {len(runs) - valid_count}")
    print(f"Input fingerprint (SHA-256): {input_fingerprint}")
    print(f"Output: {args.output.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

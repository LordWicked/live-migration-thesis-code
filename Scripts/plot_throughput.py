#!/usr/bin/env python3

# OpenAI Codex was used to generate this plotting script. 
# The generated code and the resulting plots were subsequently reviewed and validated by the author
# 
# It is secondary to the work of the Bachelor's Thesis "An Analysis of Live Migration and Its Security Implications" 
# and serves to aggregate the data which the author's benchmark scripts generate in the form of logs and timestamped .csv files.
 



import argparse
import csv
import fnmatch
import json
import re
from statistics import median, quantiles
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import MultipleLocator


RUN_FILE_RE = re.compile(r"bench-run-(\d+)(?:-(preshut|final))?\.csv")
PGSTATS_RUN_FILE_RE = re.compile(r"bench-run-(\d+)(?:-(preshut|final))?-pgstats\.csv")
HOST_STATS_RUN_FILE_RE = re.compile(r"host-stats-run-(\d+)\.csv")
SEGMENT_ORDER = {
    None: 0,
    "preshut": 0,
    "final": 1,
}
NOTE_BBOX = {
    "boxstyle": "round,pad=0.3",
    "facecolor": "white",
    "edgecolor": "0.8",
    "alpha": 0.85,
}
PLOT_CHOICES = ("throughput", "latency", "hit-rate", "host-stat")


def run_number(path):
    match = RUN_FILE_RE.fullmatch(path.name)
    if match is None:
        raise ValueError(f"Not a benchmark run CSV: {path}")
    return int(match.group(1))


def find_run_files(scenario_dir, glob_pattern="bench-run-*.csv", run_file_re=RUN_FILE_RE):
    grouped = defaultdict(list)

    for path in scenario_dir.glob(glob_pattern):
        match = run_file_re.fullmatch(path.name)
        if match is None:
            continue

        run = int(match.group(1))
        segment = match.group(2)
        grouped[run].append((SEGMENT_ORDER[segment], path))

    return [
        (run, [path for _, path in sorted(segments)])
        for run, segments in sorted(grouped.items())
    ]


def read_series(path, x_column, y_column):
    xs = []
    ys = []

    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        missing = {x_column, y_column} - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{path} is missing column(s): {', '.join(sorted(missing))}")

        for row in reader:
            if not row[x_column] or not row[y_column]:
                continue
            xs.append(float(row[x_column]))
            ys.append(float(row[y_column]))

    return xs, ys


def read_host_series(path, y_column):
    xs = []
    ys = []

    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = {
            name.strip()
            for name in (reader.fieldnames or [])
        }
        missing = {"t_since_run_start", y_column} - fieldnames
        if missing:
            raise ValueError(
                f"{path} is missing column(s): {', '.join(sorted(missing))}"
            )

        for raw_row in reader:
            row = {
                key.strip(): value.strip()
                for key, value in raw_row.items()
                if key is not None and value is not None
            }
            if not row.get("t_since_run_start") or not row.get(y_column):
                continue
            xs.append(float(row["t_since_run_start"]))
            ys.append(float(row[y_column]))

    return xs, ys


def find_host_stats_files(scenario_dir):
    files = []
    for path in scenario_dir.glob("host-stats-run-*.csv"):
        match = HOST_STATS_RUN_FILE_RE.fullmatch(path.name)
        if match is not None:
            files.append((int(match.group(1)), path))
    return sorted(files)


def read_benchmark_starts(scenario_dir, timing_csv=None):
    if timing_csv is None:
        timing_csv = scenario_dir / f"{scenario_name(scenario_dir)}.csv"
    if not timing_csv.exists():
        return {}

    benchmark_starts = {}
    with timing_csv.open(newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"run", "benchmark_start"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            return {}
        for row in reader:
            if row["run"] and row["benchmark_start"]:
                benchmark_starts[int(row["run"])] = float(row["benchmark_start"])
    return benchmark_starts


def collect_host_series(
    scenario_dir,
    y_column,
    timing_csv=None,
    divisor=1.0,
    excluded_runs=None,
):
    host_files = find_host_stats_files(scenario_dir)
    benchmark_starts = read_benchmark_starts(scenario_dir, timing_csv)
    excluded_runs = set(excluded_runs or [])
    all_series = []

    for run, path in host_files:
        if run in excluded_runs:
            continue
        if run not in benchmark_starts:
            raise ValueError(f"Missing benchmark_start for run {run} in {scenario_dir}")
        xs, ys = read_host_series(path, y_column)
        benchmark_start = benchmark_starts[run]
        aligned = [
            (x - benchmark_start, y / divisor)
            for x, y in zip(xs, ys)
            if x >= benchmark_start
        ]
        if aligned:
            all_series.append((
                [x for x, _ in aligned],
                [y for _, y in aligned],
            ))

    return all_series


def add_zero_restart_flatline(combined_xs, combined_ys, restart_marker, resumed_x, step=1.0):
    if restart_marker is None or restart_marker["prepared"]:
        return

    shutdown_x = restart_marker["shutdown_request"]
    if resumed_x <= shutdown_x:
        return

    if combined_xs and combined_xs[-1] > shutdown_x:
        return

    if combined_ys:
        combined_xs.append(shutdown_x)
        combined_ys.append(combined_ys[-1])

    zero_xs = [shutdown_x]
    next_x = (int(shutdown_x / step) + 1) * step
    while next_x < resumed_x:
        zero_xs.append(next_x)
        next_x += step
    zero_xs.append(resumed_x)

    combined_xs.extend(zero_xs)
    combined_ys.extend([0.0] * len(zero_xs))


def add_zero_stop_copy_gap(xs, ys, gap):
    if not xs or gap is None:
        return xs, ys

    start, end = gap
    if end <= start:
        return xs, ys

    points = [
        (x, y)
        for x, y in zip(xs, ys)
        if x < start or x > end
    ]
    points.extend([(start, 0.0), (end, 0.0)])
    points.sort()
    return [x for x, _ in points], [y for _, y in points]


def add_nan_restart_gap(xs, *value_series, markers):
    if not xs or not markers:
        return (xs, *value_series)

    gap_start = mean([marker["shutdown_request"] for marker in markers])
    gap_end = mean([marker["resume"] for marker in markers])
    if gap_end <= gap_start:
        return (xs, *value_series)

    points = [
        (x, *values)
        for x, values in zip(xs, zip(*value_series))
    ]
    nan_values = (float("nan"),) * len(value_series)
    points.extend([
        (gap_start, *nan_values),
        (gap_end, *nan_values),
    ])
    points.sort(key=lambda point: point[0])

    result_xs = [point[0] for point in points]
    result_series = [
        [point[index + 1] for point in points]
        for index in range(len(value_series))
    ]
    return (result_xs, *result_series)


def update_controlled_switchover_time(restart_marker, source_x, destination_x):
    if restart_marker is None:
        return
    if source_x is None or destination_x is None:
        return

    duration = destination_x - source_x
    if duration < 0.0:
        return

    previous = restart_marker.get("controlled_switchover_time")
    if previous is None:
        restart_marker["controlled_switchover_time"] = duration


def read_combined_series(
    paths,
    x_column,
    y_column,
    restart_marker=None,
    zero_restart_gap=False,
    run_file_re=RUN_FILE_RE,
):
    combined_xs = []
    combined_ys = []
    offset = 0.0

    for path in paths:
        xs, ys = read_series(path, x_column, y_column)
        if not xs:
            continue

        match = run_file_re.fullmatch(path.name)
        segment = match.group(2) if match else None
        if segment == "final" and restart_marker is not None:
            offset = restart_marker["resume"]

        adjusted_xs = [x + offset for x in xs]
        if segment == "final":
            source_x = combined_xs[-1] if combined_xs else None
            update_controlled_switchover_time(
                restart_marker,
                source_x,
                adjusted_xs[0],
            )
        if segment == "final" and zero_restart_gap:
            add_zero_restart_flatline(combined_xs, combined_ys, restart_marker, adjusted_xs[0])

        combined_xs.extend(adjusted_xs)
        combined_ys.extend(ys)
        offset = adjusted_xs[-1]

    return combined_xs, combined_ys


def mean_by_elapsed(all_series):
    buckets = defaultdict(list)
    for xs, ys in all_series:
        for x, y in zip(xs, ys):
            buckets[x].append(y)

    mean_xs = sorted(buckets)
    mean_ys = [sum(buckets[x]) / len(buckets[x]) for x in mean_xs]
    return mean_xs, mean_ys


def median_and_iqr_by_elapsed(
    all_series,
    bucket_size=1.0,
    omit_incomplete_tail=False,
    min_runs=None,
):
    buckets = defaultdict(list)
    contributing_runs = defaultdict(set)
    for run_index, (xs, ys) in enumerate(all_series):
        for x, y in zip(xs, ys):
            bucket = round(x / bucket_size) * bucket_size
            buckets[bucket].append(y)
            contributing_runs[bucket].add(run_index)

    median_xs = sorted(buckets)
    if omit_incomplete_tail:
        run_count = len(all_series)
        required_runs = min_runs if min_runs is not None else run_count
        if required_runs > run_count:
            raise ValueError(
                f"Minimum median runs ({required_runs}) exceeds available runs ({run_count})"
            )
        while (
            median_xs
            and len(contributing_runs[median_xs[-1]]) < required_runs
        ):
            median_xs.pop()

    median_ys = [median(buckets[x]) for x in median_xs]
    lower_quartiles = []
    upper_quartiles = []
    for x in median_xs:
        values = buckets[x]
        if len(values) == 1:
            q1 = q3 = values[0]
        else:
            q1, _, q3 = quantiles(values, n=4, method="inclusive")
        lower_quartiles.append(q1)
        upper_quartiles.append(q3)

    return median_xs, median_ys, lower_quartiles, upper_quartiles


def mean(values):
    return sum(values) / len(values)


def format_duration(seconds):
    if seconds < 1.0:
        return f"{seconds * 1000.0:.1f} ms"
    return f"{seconds:.2f} s"


def draw_note(lines):
    if not lines:
        return

    plt.gca().text(
        0.01,
        0.98,
        "\n".join(lines),
        transform=plt.gca().transAxes,
        ha="left",
        va="top",
        fontsize="small",
        bbox=NOTE_BBOX,
    )


def draw_grouped_legend(plot_handles, plot_labels, event_handles, event_labels):
    if not event_handles:
        plt.legend(
            plot_handles,
            plot_labels,
            loc="upper right",
            fontsize="small",
        )
        return

    row_count = max(len(plot_handles), len(event_handles))
    blank_handle = Line2D([], [], linestyle="none", alpha=0.0)
    plot_padding = row_count - len(plot_handles)
    event_padding = row_count - len(event_handles)
    handles = (
        plot_handles
        + [blank_handle] * plot_padding
        + event_handles
        + [blank_handle] * event_padding
    )
    labels = (
        plot_labels
        + [""] * plot_padding
        + event_labels
        + [""] * event_padding
    )
    plt.legend(
        handles,
        labels,
        loc="upper right",
        ncol=2,
        fontsize="small",
    )


def draw_migration_note(downtimes, migration_durations):
    note_lines = []
    if downtimes:
        note_lines.append(f"Median downtime window: {format_duration(median(downtimes))}")
    if migration_durations:
        note_lines.append(f"Median QMP migration duration: {format_duration(median(migration_durations))}")
    draw_note(note_lines)


def preparation_phase_durations(markers):
    return [
        marker["promotion_done"] - marker["postgres2_ready"]
        for marker in markers
        if marker["prepared"]
        and marker["promotion_done"] is not None
        and marker["postgres2_ready"] is not None
        and marker["promotion_done"] >= marker["postgres2_ready"]
    ]


def draw_restart_note(
    restart_durations,
    label="Median service interruption",
    preparation_durations=None,
):
    note_lines = []
    if restart_durations:
        note_lines.append(f"{label}: {format_duration(median(restart_durations))}")
    if preparation_durations:
        note_lines.append(
            f"WAL catch-up duration: {format_duration(median(preparation_durations))}"
        )
    draw_note(note_lines)


def read_downtime_seconds(path):
    if not path.exists():
        return None

    with path.open() as handle:
        samples = json.load(handle)

    for sample in reversed(samples):
        mig = sample.get("query_migrate", sample)
        downtime_ms = mig.get("downtime")
        if downtime_ms is not None:
            return float(downtime_ms) / 1000.0

    return None


def read_qmp_duration_seconds(path):
    if not path.exists():
        return None

    with path.open() as handle:
        samples = json.load(handle)

    for sample in reversed(samples):
        migration = sample.get("query_migrate", sample)
        ram = migration.get("ram")
        total_time_ms = migration.get("total-time")
        if (
            migration.get("status") == "completed"
            and isinstance(ram, dict)
            and ram
            and total_time_ms is not None
        ):
            return float(total_time_ms) / 1000.0

    return None


def read_scenario_flags(scenario_dir):
    spec_path = scenario_dir / f"specs_{scenario_name(scenario_dir)}"
    flags = {
        "prewarmed": False,
    }

    if not spec_path.exists():
        return flags

    with spec_path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            key, _, value = line.partition(":")
            normalized_value = value.strip().lower()
            if key.strip() == "prewarmed":
                flags["prewarmed"] = normalized_value in {"1", "true", "yes"}

    return flags


def read_stop_copy_gap(scenario_dir, run, benchmark_start):
    event_path = scenario_dir / f"events-run-{run}.csv"
    if not event_path.exists():
        return None

    events = {}
    with event_path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"t_since_run_start", "event"}
        if required - set(reader.fieldnames or []):
            return None

        for row in reader:
            event = row["event"]
            if event in {"source_stopped", "destination_resumed"} and row["t_since_run_start"]:
                events[event] = float(row["t_since_run_start"]) - benchmark_start

    start = events.get("source_stopped")
    end = events.get("destination_resumed")
    if start is None or end is None or end <= start:
        return None
    return start, end


def read_timing_markers(scenario_dir, timing_csv):
    if timing_csv is None:
        timing_csv = scenario_dir / f"{scenario_name(scenario_dir)}.csv"

    if not timing_csv.exists():
        return []

    markers = []
    with timing_csv.open(newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"run", "benchmark_start", "migration_start", "migration_end"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            return []

        for row in reader:
            run = int(row["run"])
            qmp_path = scenario_dir / f"mig-stats-run-{run}.json"
            benchmark_start = float(row["benchmark_start"])
            destination_boot = float(row["destination_boot"]) - benchmark_start if row.get("destination_boot") else None
            migration_start = float(row["migration_start"]) - benchmark_start
            migration_end = float(row["migration_end"]) - benchmark_start
            # downtime = read_downtime_seconds(scenario_dir / f"mig-stats-run-{run}.json")
            stop_copy_gap = read_stop_copy_gap(scenario_dir, run, benchmark_start) # falls downtime außerhalb stop-copy nicht mehr stimmt ist das hier der Übeltäter
            downtime = (
                stop_copy_gap[1] - stop_copy_gap[0]
                if stop_copy_gap is not None
                else read_downtime_seconds(qmp_path)
            )

            markers.append({
                "run": run,
                "destination_boot": destination_boot,
                "migration_start": migration_start,
                "migration_end": migration_end,
                "migration_duration_qmp": read_qmp_duration_seconds(qmp_path),
                "downtime": downtime,
                # "stop_copy_gap": read_stop_copy_gap(scenario_dir, run, benchmark_start),
                "stop_copy_gap": stop_copy_gap,
            })

    return markers


def read_restart_markers(scenario_dir, timing_csv):
    if timing_csv is None:
        timing_csv = scenario_dir / f"{scenario_name(scenario_dir)}.csv"

    if not timing_csv.exists():
        return []

    scenario_flags = read_scenario_flags(scenario_dir)
    markers = []
    with timing_csv.open(newline="") as handle:
        reader = csv.DictReader(handle)
        required = {
            "run",
            "benchmark_start",
            "shutdown_request",
            "shutdown_finished",
            "ssh2_ready",
            "postgres2_ready",
        }
        missing = required - set(reader.fieldnames or [])
        if missing:
            return []

        for row in reader:
            shutdown_request_raw = float(row["shutdown_request"])
            postgres2_ready_raw = float(row["postgres2_ready"])
            ssh2_ready_raw = float(row["ssh2_ready"])
            standby_ready_raw = float(row.get("standby_ready") or 0.0)
            promotion_start_raw = float(row.get("promotion_start") or 0.0)
            promotion_done_raw = float(row.get("promotion_done") or 0.0)
            benchmark_resume_raw = float(
                row.get("benchmark_resume")
                or row.get("destination_benchmark_start")
                or 0.0
            )
            prepared = ssh2_ready_raw < shutdown_request_raw
            resume_raw = (
                benchmark_resume_raw
                if benchmark_resume_raw > 0.0
                else promotion_done_raw
                if prepared and promotion_done_raw > 0.0
                else postgres2_ready_raw
            )

            if shutdown_request_raw <= 0.0 or resume_raw <= 0.0:
                continue

            benchmark_start = float(row["benchmark_start"])
            markers.append({
                "run": int(row["run"]),
                "prepared": prepared,
                "prewarmed": scenario_flags["prewarmed"],
                "shutdown_request": shutdown_request_raw - benchmark_start,
                "shutdown_finished": float(row["shutdown_finished"]) - benchmark_start,
                "ssh2_ready": ssh2_ready_raw - benchmark_start,
                "postgres2_ready": postgres2_ready_raw - benchmark_start,
                "standby_ready": (
                    standby_ready_raw - benchmark_start
                    if standby_ready_raw > 0.0
                    else None
                ),
                "promotion_start": (
                    promotion_start_raw - benchmark_start
                    if promotion_start_raw > 0.0
                    else None
                ),
                "promotion_done": (
                    promotion_done_raw - benchmark_start
                    if promotion_done_raw > 0.0
                    else None
                ),
                "benchmark_resume": (
                    benchmark_resume_raw - benchmark_start
                    if benchmark_resume_raw > 0.0
                    else None
                ),
                "resume": resume_raw - benchmark_start,
            })

    return markers


def restart_postgres_label(markers):
    if all(marker["prepared"] for marker in markers):
        return "YCSB resumed"
    if all(marker["prewarmed"] for marker in markers):
        return "YCSB resumed"
    return "YCSB resumed"


def restart_ssh_label(markers):
    if all(marker["prepared"] for marker in markers):
        return "Standby SSH ready"
    return "SSH ready after restart"


def draw_timing_markers(markers, mode):
    if not markers:
        return

    if mode == "average":
        destination_boots = [
            marker["destination_boot"] for marker in markers
            if marker["destination_boot"] is not None
        ]
        migration_start = mean([marker["migration_start"] for marker in markers])
        migration_end = mean([marker["migration_end"] for marker in markers])
        downtimes = [
            marker["downtime"] for marker in markers
            if marker["downtime"] is not None
        ]
        stop_copy_gaps = [
            marker["stop_copy_gap"] for marker in markers
            if marker.get("stop_copy_gap") is not None
        ]
        migration_durations = [
            marker["migration_duration_qmp"]
            for marker in markers
            if marker["migration_duration_qmp"] is not None
        ]

        if destination_boots:
            plt.axvline(
                mean(destination_boots),
                color="tab:purple",
                linestyle="-.",
                linewidth=2.0,
                label="Destination boot",
            )

        plt.axvline(
            migration_start,
            color="tab:blue",
            linestyle=":",
            linewidth=2.0,
            label="Migration start",
        )

        if stop_copy_gaps:
            plt.axvspan(
                mean([gap[0] for gap in stop_copy_gaps]),
                mean([gap[1] for gap in stop_copy_gaps]),
                color="tab:red",
                alpha=0.12,
                label="Downtime window",
            )
        elif downtimes:
            downtime = median(downtimes)
            plt.axvspan(
                migration_end - downtime,
                migration_end,
                color="tab:red",
                alpha=0.12,
                label="Downtime window",
            )

        plt.axvline(
            migration_end,
            color="tab:red",
            linestyle="--",
            linewidth=2.0,
            label="Migration complete",
        )
        draw_migration_note(downtimes, migration_durations)
        return

    labels = {
        "destination_boot": "Destination boot",
        "migration_start": "Migration start",
        "downtime": "Downtime window",
        "migration_end": "Migration complete",
    }
    downtimes = [
        marker["downtime"] for marker in markers
        if marker["downtime"] is not None
    ]
    migration_durations = [
        marker["migration_duration_qmp"]
        for marker in markers
        if marker["migration_duration_qmp"] is not None
    ]

    for marker in markers:
        if marker["destination_boot"] is not None:
            plt.axvline(
                marker["destination_boot"],
                color="tab:purple",
                linestyle="-.",
                linewidth=1.0,
                alpha=0.35,
                label=labels.pop("destination_boot", None),
            )

        plt.axvline(
            marker["migration_start"],
            color="tab:blue",
            linestyle=":",
            linewidth=1.0,
            alpha=0.35,
            label=labels.pop("migration_start", None),
        )

        stop_copy_gap = marker.get("stop_copy_gap")
        if stop_copy_gap is not None:
            gap_start, gap_end = stop_copy_gap
            plt.axvspan(
                gap_start,
                gap_end,
                color="tab:red",
                alpha=0.05,
                label=labels.pop("downtime", None),
            )
        elif marker["downtime"] is not None:
            plt.axvspan(
                marker["migration_end"] - marker["downtime"],
                marker["migration_end"],
                color="tab:red",
                alpha=0.05,
                label=labels.pop("downtime", None),
            )

        plt.axvline(
            marker["migration_end"],
            color="tab:red",
            linestyle="--",
            linewidth=1.0,
            alpha=0.35,
            label=labels.pop("migration_end", None),
        )

    draw_migration_note(downtimes, migration_durations)


def draw_restart_markers(markers, mode):
    if not markers:
        return

    if mode == "average":
        shutdown_request = mean([marker["shutdown_request"] for marker in markers])
        shutdown_finished = mean([marker["shutdown_finished"] for marker in markers])
        ssh2_ready = mean([marker["ssh2_ready"] for marker in markers])
        postgres2_ready = mean([marker["postgres2_ready"] for marker in markers])
        resume = mean([marker["resume"] for marker in markers])
        prepared = all(marker["prepared"] for marker in markers)
        promotion_starts = [
            marker["promotion_start"] for marker in markers
            if marker["promotion_start"] is not None
        ]
        standby_readies = [
            marker["standby_ready"] for marker in markers
            if marker["standby_ready"] is not None
        ]
        promotion_dones = [
            marker["promotion_done"] for marker in markers
            if marker["promotion_done"] is not None
        ]
        controlled_switchover_times = [
            marker["controlled_switchover_time"] for marker in markers
            if marker.get("controlled_switchover_time") is not None
        ]
        postgres_label = restart_postgres_label(markers)
        ssh_label = restart_ssh_label(markers)
        restart_durations = controlled_switchover_times or [
            marker["resume"] - marker["shutdown_request"] for marker in markers
        ]

        plt.axvline(
            shutdown_request,
            color="tab:orange",
            linestyle=":",
            linewidth=2.0,
            label="Shutdown requested",
        )
        plt.axvline(
            shutdown_finished,
            color="tab:red",
            linestyle="--",
            linewidth=2.0,
            label="Shutdown finished",
        )
        plt.axvspan(
            resume - median(controlled_switchover_times)
            if controlled_switchover_times
            else shutdown_request,
            resume,
            color="tab:red",
            alpha=0.12,
            label="Switchover time" if prepared else "Restart gap",
        )
        plt.axvline(
            ssh2_ready,
            color="tab:purple",
            linestyle=":",
            linewidth=1.4 if prepared else 2.0,
            alpha=0.45 if prepared else 1.0,
            label=ssh_label,
        )
        if prepared:
            plt.axvline(
                postgres2_ready,
                color="tab:green",
                linestyle=":",
                linewidth=1.4,
                alpha=0.45,
                label="Standby postgres ready",
            )
            if standby_readies:
                plt.axvline(
                    mean(standby_readies),
                    color="tab:cyan",
                    linestyle="-.",
                    linewidth=2.0,
                    label="Standby ready (WAL lag check done)",
                )
            if promotion_starts:
                plt.axvline(
                    mean(promotion_starts),
                    color="tab:blue",
                    linestyle="-.",
                    linewidth=2.0,
                    label="Promotion start",
                )
            if promotion_dones:
                plt.axvline(
                    mean(promotion_dones),
                    color="tab:blue",
                    linestyle="--",
                    linewidth=2.0,
                    label="Promotion complete",
                )
        plt.axvline(
            resume,
            color="tab:green",
            linestyle="--",
            linewidth=2.0,
            label=postgres_label,
        )
        note_label = (
            "Median service interruption"
            if prepared
            else "Median service interruption"
        )
        draw_restart_note(
            restart_durations,
            note_label,
            preparation_phase_durations(markers) if prepared else None,
        )
        return

    labels = {
        "shutdown_request": "shutdown requested",
        "shutdown_finished": "shutdown finished",
        "restart_gap": "controlled switchover time" if all(marker["prepared"] for marker in markers) else "restart gap",
        "ssh2_ready": restart_ssh_label(markers),
        "postgres2_ready": "standby postgres ready",
        "standby_ready": "standby ready (WAL lag check done)",
        "promotion_start": "promotion start",
        "promotion_done": "promotion complete",
        "resume": restart_postgres_label(markers),
    }
    restart_durations = [
        marker.get("controlled_switchover_time")
        if marker.get("controlled_switchover_time") is not None
        else marker["resume"] - marker["shutdown_request"]
        for marker in markers
    ]
    for marker in markers:
        plt.axvline(
            marker["shutdown_request"],
            color="tab:orange",
            linestyle=":",
            linewidth=1.0,
            alpha=0.35,
            label=labels.pop("shutdown_request", None),
        )
        plt.axvline(
            marker["shutdown_finished"],
            color="tab:red",
            linestyle="--",
            linewidth=1.0,
            alpha=0.35,
            label=labels.pop("shutdown_finished", None),
        )
        plt.axvspan(
            marker["resume"] - marker["controlled_switchover_time"]
            if marker.get("controlled_switchover_time") is not None
            else marker["shutdown_request"],
            marker["resume"],
            color="tab:red",
            alpha=0.05,
            label=labels.pop("restart_gap", None),
        )
        plt.axvline(
            marker["ssh2_ready"],
            color="tab:purple",
            linestyle=":",
            linewidth=1.0,
            alpha=0.18 if marker["prepared"] else 0.35,
            label=labels.pop("ssh2_ready", None),
        )
        if marker["prepared"]:
            plt.axvline(
                marker["postgres2_ready"],
                color="tab:green",
                linestyle=":",
                linewidth=1.0,
                alpha=0.18,
                label=labels.pop("postgres2_ready", None),
            )
            if marker["promotion_start"] is not None:
                plt.axvline(
                    marker["promotion_start"],
                    color="tab:blue",
                    linestyle="-.",
                    linewidth=1.0,
                    alpha=0.35,
                    label=labels.pop("promotion_start", None),
                )
            if marker["standby_ready"] is not None:
                plt.axvline(
                    marker["standby_ready"],
                    color="tab:cyan",
                    linestyle="-.",
                    linewidth=1.0,
                    alpha=0.35,
                    label=labels.pop("standby_ready", None),
                )
            if marker["promotion_done"] is not None:
                plt.axvline(
                    marker["promotion_done"],
                    color="tab:blue",
                    linestyle="--",
                    linewidth=1.0,
                    alpha=0.35,
                    label=labels.pop("promotion_done", None),
                )
        plt.axvline(
            marker["resume"],
            color="tab:green",
            linestyle="--",
            linewidth=1.0,
            alpha=0.35,
            label=labels.pop("resume", None),
        )
    note_label = (
        "Median Median service interruption"
        if all(marker["prepared"] for marker in markers)
        else "Median service interruption"
    )
    draw_restart_note(
        restart_durations,
        note_label,
        preparation_phase_durations(markers),
    )


def scenario_name(path):
    return path.resolve().name


def categorized_output_path(path, category):
    return path.parent / category / path.name


def plot_output_path(output, scenario_dir, category, slug, median_plot=False):
    if output is None:
        path = scenario_dir / f"{scenario_name(scenario_dir)}-{slug}.png"
    elif slug == "throughput":
        path = output
    else:
        path = output.with_name(f"{output.stem}-{slug}{output.suffix}")

    if median_plot:
        path = path.with_name(f"{path.stem}-median{path.suffix}")
        category = f"{category}-median"
    return categorized_output_path(path, category)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Plot benchmark metrics for one scenario or a batch of scenario directories."
    )
    parser.add_argument(
        "scenario_dir",
        nargs="?",
        type=Path,
        help="Scenario log directory, e.g. Scripts/logs_bigbench/post_10",
    )
    parser.add_argument(
        "--batch",
        type=Path,
        help="Log root containing scenario directories. Replaces plot_all_throughput.sh.",
    )
    parser.add_argument(
        "--destination",
        type=Path,
        help="Output root for --batch. Defaults to writing plots next to each scenario.",
    )
    parser.add_argument(
        "--scenario",
        action="append",
        default=[],
        help="Scenario name or shell-style glob to include in --batch. Can be repeated.",
    )
    parser.add_argument(
        "--exclude-scenario",
        action="append",
        default=[],
        help="Scenario name or shell-style glob to exclude in --batch. Can be repeated.",
    )
    parser.add_argument(
        "--exclude-run",
        action="append",
        type=int,
        default=[],
        help="Run number to exclude from the primary scenario. Can be repeated.",
    )
    parser.add_argument(
        "--list-scenarios",
        action="store_true",
        help="List scenarios selected by --batch and exit.",
    )
    parser.add_argument(
        "--plots",
        nargs="+",
        choices=PLOT_CHOICES,
        default=list(PLOT_CHOICES),
        help="Plots to produce. Default: throughput latency hit-rate",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Output image path. Defaults to <scenario>/throughput/<scenario>-throughput.png",
    )
    parser.add_argument(
        "--x-column",
        default="elapsed_sec",
        help="CSV column for the x-axis. Default: elapsed_sec",
    )
    parser.add_argument(
        "--y-column",
        default="current_ops_per_sec",
        help="CSV column for throughput. Default: current_ops_per_sec",
    )
    parser.add_argument(
        "--host-column",
        default="disk_write_bytes_per_s",
        help="Column from host-stats-run-*.csv for host-stat plots. Default: disk_write_bytes_per_s",
    )
    parser.add_argument(
        "--host-divisor",
        type=float,
        default=1.0,
        help="Divide host-stat values by this number before plotting.",
    )
    parser.add_argument(
    "--x-label",
    default="Elapsed time [s]",
    help="Displayed x-axis label.",
    )
    parser.add_argument(
        "--y-label",
        help="Displayed y-axis label. Defaults depend on the selected plot.",
    )
    parser.add_argument(
        "--title",
        help="Plot title. Defaults to '<scenario> throughput'",
    )
    parser.add_argument(
        "--font-size",
        type=float,
        help="Base font size for plot text.",
    )
    parser.add_argument(
        "--x-range",
        type=float,
        nargs=2,
        metavar=("MIN", "MAX"),
        help="Displayed x-axis range, e.g. --x-range 0 200.",
    )
    parser.add_argument(
        "--y-range",
        type=float,
        nargs=2,
        metavar=("MIN", "MAX"),
        help="Displayed y-axis range, e.g. --y-range 0 100000.",
    )
    parser.add_argument(
        "--x-tick-step",
        type=float,
        help="Spacing between major x-axis ticks, e.g. --x-tick-step 20.",
    )
    parser.add_argument(
        "--no-mean",
        action="store_true",
        help="Do not draw the mean throughput line.",
    )
    parser.add_argument(
        "--no-median-plot",
        action="store_true",
        help="Do not write the additional median-only throughput/latency plots.",
    )
    parser.add_argument(
        "--no-iqr",
        action="store_true",
        help="Do not draw the Q1-Q3 shaded band on median plots.",
    )
    parser.add_argument(
        "--no-baseline-iqr",
        action="store_true",
        help="Do not draw the baseline Q1-Q3 shaded band; keep the scenario IQR and baseline median line.",
    )
    parser.add_argument(
        "--omit-incomplete-median-tail",
        action="store_true",
        help="Omit trailing median/IQR buckets that are not represented by every run.",
    )
    parser.add_argument(
        "--min-median-runs",
        type=int,
        help="With --omit-incomplete-median-tail, retain tail buckets represented by at least this many runs.",
    )
    parser.add_argument(
        "--no-latency-plots",
        action="store_true",
        help="Do not write additional average and p99.99 latency plots.",
    )
    parser.add_argument(
        "--zero-restart-gap",
        action="store_true",
        help="For non-prepared restart runs, insert an inferred 0-throughput flatline from shutdown request to the first resumed throughput sample.",
    )
    parser.add_argument(
        "--median-output",
        type=Path,
        help="Output image path for the median-only plot. Defaults to <scenario>/throughput-median/<scenario>-throughput-median.png, or <output-dir>/throughput-median/<output-stem>-median.png when --output is set.",
    )
    parser.add_argument(
        "--baseline-dir",
        type=Path,
        help="Baseline scenario directory whose median throughput is overlaid on the median throughput plot.",
    )
    parser.add_argument(
        "--restart-overlay-dir",
        type=Path,
        help="Restart scenario directory whose median and IQR are overlaid using its own restart timing and zero-throughput gap.",
    )
    parser.add_argument(
        "--primary-label",
        help="Series label for the primary scenario when using --restart-overlay-dir.",
    )
    parser.add_argument(
        "--restart-overlay-label",
        help="Series label for --restart-overlay-dir.",
    )
    parser.add_argument(
        "--no-timing",
        action="store_true",
        help="Do not draw migration timing markers.",
    )
    parser.add_argument(
        "--timing-csv",
        type=Path,
        help="Scenario timing CSV. Defaults to <scenario>/<scenario>.csv",
    )
    parser.add_argument(
        "--timing-mode",
        choices=("average", "all"),
        default="average",
        help="Draw averaged timing markers or markers for every run. Default: average",
    )
    args = parser.parse_args()
    if args.font_size is not None and args.font_size <= 0:
        parser.error("--font-size must be greater than zero")
    if args.x_range is not None and args.x_range[0] >= args.x_range[1]:
        parser.error("--x-range MIN must be less than MAX")
    if args.y_range is not None and args.y_range[0] >= args.y_range[1]:
        parser.error("--y-range MIN must be less than MAX")
    if args.x_tick_step is not None and args.x_tick_step <= 0:
        parser.error("--x-tick-step must be greater than zero")
    if args.host_divisor <= 0:
        parser.error("--host-divisor must be greater than zero")
    if any(run <= 0 for run in args.exclude_run):
        parser.error("--exclude-run values must be greater than zero")
    if args.min_median_runs is not None:
        if args.min_median_runs <= 0:
            parser.error("--min-median-runs must be greater than zero")
        if not args.omit_incomplete_median_tail:
            parser.error("--min-median-runs requires --omit-incomplete-median-tail")
    if args.baseline_dir is not None and args.restart_overlay_dir is not None:
        parser.error("--baseline-dir and --restart-overlay-dir are mutually exclusive")
    if args.restart_overlay_dir is None and (
        args.primary_label is not None or args.restart_overlay_label is not None
    ):
        parser.error("--primary-label and --restart-overlay-label require --restart-overlay-dir")
    if args.batch is not None and args.restart_overlay_dir is not None:
        parser.error("--restart-overlay-dir is only supported for single-scenario plotting")
    return args


def draw_markers(timing_markers, restart_markers, timing_mode):
    if timing_markers:
        draw_timing_markers(timing_markers, timing_mode)
    else:
        draw_restart_markers(restart_markers, timing_mode)


def collect_series(
    run_groups,
    x_column,
    y_column,
    restart_markers_by_run,
    zero_restart_gap,
    stop_copy_gaps_by_run=None,
    run_file_re=RUN_FILE_RE,
    drop_last_sample=False,
):
    all_series = []

    for run, paths in run_groups:
        xs, ys = read_combined_series(
            paths,
            x_column,
            y_column,
            restart_markers_by_run.get(run),
            zero_restart_gap,
            run_file_re,
        )
        if stop_copy_gaps_by_run is not None:
            xs, ys = add_zero_stop_copy_gap(xs, ys, stop_copy_gaps_by_run.get(run))
        if drop_last_sample:
            xs = xs[:-1]
            ys = ys[:-1]
        if xs:
            all_series.append((xs, ys))

    return all_series


def plot_metric(
    all_series,
    output,
    title,
    x_label,
    y_label,
    timing_markers,
    restart_markers,
    timing_mode,
    no_timing,
    x_range=None,
    y_range=None,
    x_tick_step=None,
    draw_mean=True,
    median_only=False,
    baseline_series=None,
    restart_overlay_series=None,
    restart_overlay_markers=None,
    primary_label=None,
    restart_overlay_label=None,
    blank_restart_gap=False,
    draw_iqr=True,
    draw_baseline_iqr=True,
    omit_incomplete_median_tail=False,
    min_median_runs=None,
):
    output.parent.mkdir(parents=True, exist_ok=True)

    plt.figure(figsize=(12, 6))

    if median_only:
        median_xs, median_ys, q1_ys, q3_ys = median_and_iqr_by_elapsed(
            all_series,
            omit_incomplete_tail=omit_incomplete_median_tail,
            min_runs=min_median_runs,
        )
        if blank_restart_gap:
            median_xs, median_ys, q1_ys, q3_ys = add_nan_restart_gap(
                median_xs,
                median_ys,
                q1_ys,
                q3_ys,
                markers=restart_markers,
            )
        if restart_overlay_series:
            overlay_xs, overlay_ys, overlay_q1, overlay_q3 = (
                median_and_iqr_by_elapsed(
                    restart_overlay_series,
                    omit_incomplete_tail=omit_incomplete_median_tail,
                    min_runs=min_median_runs,
                )
            )
            if blank_restart_gap:
                overlay_xs, overlay_ys, overlay_q1, overlay_q3 = add_nan_restart_gap(
                    overlay_xs,
                    overlay_ys,
                    overlay_q1,
                    overlay_q3,
                    markers=restart_overlay_markers,
                )
            plt.plot(
                overlay_xs,
                overlay_ys,
                color="tab:gray",
                linestyle="--",
                linewidth=2.0,
                label=f"{restart_overlay_label} Median",
            )
            if draw_iqr:
                plt.fill_between(
                    overlay_xs,
                    overlay_q1,
                    overlay_q3,
                    color="tab:gray",
                    alpha=0.18,
                    label=f"{restart_overlay_label} IQR",
                )

        median_label = f"{primary_label} Median" if primary_label else "Median"
        iqr_label = f"{primary_label} IQR" if primary_label else "IQR"
        plt.plot(median_xs, median_ys, color="black", linewidth=2.4, label=median_label)
        if draw_iqr:
            plt.fill_between(
                median_xs,
                q1_ys,
                q3_ys,
                color="black",
                alpha=0.25,
                label=iqr_label,
            )
        if baseline_series:
            baseline_xs, baseline_ys, baseline_q1, baseline_q3 = (
                median_and_iqr_by_elapsed(
                    baseline_series,
                    omit_incomplete_tail=omit_incomplete_median_tail,
                    min_runs=min_median_runs,
                )
            )
            plt.plot(
                baseline_xs,
                baseline_ys,
                color="tab:gray",
                linestyle="--",
                linewidth=2.0,
                label="Baseline median",
            )
            if draw_iqr and draw_baseline_iqr:
                plt.fill_between(
                    baseline_xs,
                    baseline_q1,
                    baseline_q3,
                    color="tab:gray",
                    alpha=0.12,
                    label="Baseline IQR",
                )
    else:
        for run_index, (xs, ys) in enumerate(all_series):
            if blank_restart_gap and run_index < len(restart_markers):
                xs, ys = add_nan_restart_gap(
                    xs,
                    ys,
                    markers=[restart_markers[run_index]],
                )
            plt.plot(xs, ys, linewidth=1.0, alpha=0.45, label="_nolegend_")

        if draw_mean:
            mean_xs, mean_ys = mean_by_elapsed(all_series)
            if blank_restart_gap:
                mean_xs, mean_ys = add_nan_restart_gap(
                    mean_xs,
                    mean_ys,
                    markers=restart_markers,
                )
            plt.plot(mean_xs, mean_ys, color="black", linewidth=2.4, label="Mean")

    axes = plt.gca()
    plot_handles, plot_labels = axes.get_legend_handles_labels()

    if not no_timing:
        draw_markers(timing_markers, restart_markers, timing_mode)

    all_handles, all_labels = axes.get_legend_handles_labels()
    event_handles = all_handles[len(plot_handles):]
    event_labels = all_labels[len(plot_labels):]

    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    if x_range is not None:
        plt.xlim(*x_range)
    if y_range is not None:
        plt.ylim(*y_range)
    if x_tick_step is not None:
        plt.gca().xaxis.set_major_locator(MultipleLocator(x_tick_step))
    plt.grid(True, alpha=0.25)
    draw_grouped_legend(
        plot_handles,
        plot_labels,
        event_handles,
        event_labels,
    )
    plt.tight_layout()
    plt.savefig(output, dpi=160)
    plt.close()
    print(f"Wrote {output}")


def plot_metric_set(
    series,
    args,
    scenario_dir,
    output,
    category,
    slug,
    title,
    median_title,
    y_label,
    timing_markers,
    restart_markers,
    median_output=None,
    baseline_series=None,
    restart_overlay_series=None,
    restart_overlay_markers=None,
    primary_label=None,
    restart_overlay_label=None,
    blank_restart_gap=False,
):
    plot_metric(
        series,
        plot_output_path(output, scenario_dir, category, slug),
        title,
        args.x_label,
        y_label,
        timing_markers,
        restart_markers,
        args.timing_mode,
        args.no_timing,
        x_range=args.x_range,
        y_range=args.y_range,
        x_tick_step=args.x_tick_step,
        draw_mean=not args.no_mean,
        blank_restart_gap=blank_restart_gap,
    )

    if args.no_median_plot:
        return

    plot_metric(
        series,
        median_output or plot_output_path(output, scenario_dir, category, slug, median_plot=True),
        median_title,
        args.x_label,
        y_label,
        timing_markers,
        restart_markers,
        args.timing_mode,
        args.no_timing,
        x_range=args.x_range,
        y_range=args.y_range,
        x_tick_step=args.x_tick_step,
        median_only=True,
        baseline_series=baseline_series,
        restart_overlay_series=restart_overlay_series,
        restart_overlay_markers=restart_overlay_markers,
        primary_label=primary_label,
        restart_overlay_label=restart_overlay_label,
        blank_restart_gap=blank_restart_gap,
        draw_iqr=not args.no_iqr,
        draw_baseline_iqr=not args.no_baseline_iqr,
        omit_incomplete_median_tail=args.omit_incomplete_median_tail,
        min_median_runs=args.min_median_runs,
    )


def scenario_matches(name, includes, excludes):
    included = not includes or any(fnmatch.fnmatchcase(name, pattern) for pattern in includes)
    excluded = any(fnmatch.fnmatchcase(name, pattern) for pattern in excludes)
    return included and not excluded


def has_benchmark_runs(scenario_dir):
    return bool(find_run_files(scenario_dir))


def selected_scenario_dirs(logs_dir, includes, excludes):
    if not logs_dir.is_dir():
        raise SystemExit(f"Batch log directory does not exist: {logs_dir}")

    return [
        path for path in sorted(logs_dir.iterdir())
        if path.is_dir()
        and scenario_matches(path.name, includes, excludes)
        and has_benchmark_runs(path)
    ]


def output_for_scenario(args, scenario_dir):
    if args.destination is None:
        return args.output
    return args.destination / f"{scenario_name(scenario_dir)}.png" # TODO -throughput.png


def selected_plots(args):
    plots = set(args.plots)
    if args.no_latency_plots:
        plots.discard("latency")
    return plots


def plot_scenario(args, scenario_dir, output=None):
    if not scenario_dir.is_dir():
        raise SystemExit(f"Scenario directory does not exist: {scenario_dir}")

    excluded_runs = set(args.exclude_run)
    run_groups = [
        group
        for group in find_run_files(scenario_dir)
        if group[0] not in excluded_runs
    ]
    if not run_groups:
        raise SystemExit(f"No included bench-run-*.csv files found in {scenario_dir}")

    timing_markers = []
    restart_markers = []
    if not args.no_timing:
        timing_markers = read_timing_markers(scenario_dir, args.timing_csv)
        if not timing_markers:
            restart_markers = read_restart_markers(scenario_dir, args.timing_csv)
    timing_markers = [
        marker for marker in timing_markers
        if marker["run"] not in excluded_runs
    ]
    restart_markers = [
        marker for marker in restart_markers
        if marker["run"] not in excluded_runs
    ]
    restart_markers_by_run = {
        marker["run"]: marker
        for marker in restart_markers
    }
    if restart_markers:
        collect_series(
            run_groups,
            "elapsed_sec",
            "current_ops_per_sec",
            restart_markers_by_run,
            zero_restart_gap=False,
            drop_last_sample=True,
        )
    stop_copy_gaps_by_run = {
        marker["run"]: marker["stop_copy_gap"]
        for marker in timing_markers
        if marker.get("stop_copy_gap") is not None
    }

    name = scenario_name(scenario_dir)
    plots = selected_plots(args)
    wrote_plot = False
    overlay_run_groups = []
    overlay_restart_markers = []
    overlay_restart_markers_by_run = {}
    primary_label = None
    restart_overlay_label = None

    if args.restart_overlay_dir is not None:
        if not args.restart_overlay_dir.is_dir():
            raise SystemExit(
                f"Restart overlay scenario directory does not exist: {args.restart_overlay_dir}"
            )
        overlay_run_groups = find_run_files(args.restart_overlay_dir)
        if not overlay_run_groups:
            raise SystemExit(
                f"No bench-run-*.csv files found in restart overlay directory {args.restart_overlay_dir}"
            )
        overlay_restart_markers = read_restart_markers(
            args.restart_overlay_dir,
            timing_csv=None,
        )
        if not overlay_restart_markers:
            raise SystemExit(
                f"No restart timing markers found in overlay directory {args.restart_overlay_dir}"
            )
        overlay_restart_markers_by_run = {
            marker["run"]: marker
            for marker in overlay_restart_markers
        }
        collect_series(
            overlay_run_groups,
            "elapsed_sec",
            "current_ops_per_sec",
            overlay_restart_markers_by_run,
            zero_restart_gap=False,
            drop_last_sample=True,
        )
        primary_label = args.primary_label or name
        restart_overlay_label = (
            args.restart_overlay_label
            or scenario_name(args.restart_overlay_dir)
        )

    if "throughput" in plots:
        all_series = collect_series(
            run_groups,
            args.x_column,
            args.y_column,
            restart_markers_by_run,
            args.zero_restart_gap,
            stop_copy_gaps_by_run=stop_copy_gaps_by_run,
            drop_last_sample=True,
        )

        baseline_series = None
        restart_overlay_series = None
        if args.baseline_dir is not None:
            if not args.baseline_dir.is_dir():
                raise SystemExit(f"Baseline scenario directory does not exist: {args.baseline_dir}")
            baseline_run_groups = find_run_files(args.baseline_dir)
            if not baseline_run_groups:
                raise SystemExit(f"No bench-run-*.csv files found in baseline directory {args.baseline_dir}")
            baseline_series = collect_series(
                baseline_run_groups,
                args.x_column,
                args.y_column,
                restart_markers_by_run={},
                zero_restart_gap=False,
                drop_last_sample=True,
            )
            if not baseline_series:
                raise SystemExit(f"No plottable baseline throughput data found in {args.baseline_dir}")

        if overlay_run_groups:
            restart_overlay_series = collect_series(
                overlay_run_groups,
                args.x_column,
                args.y_column,
                overlay_restart_markers_by_run,
                zero_restart_gap=True,
                drop_last_sample=True,
            )
            if not restart_overlay_series:
                raise SystemExit(
                    f"No plottable restart throughput data found in {args.restart_overlay_dir}"
                )

        if all_series:
            plot_metric_set(
                all_series,
                args,
                scenario_dir,
                output,
                "throughput",
                "throughput",
                args.title if args.title is not None else f"{name} throughput",
                args.title if args.title is not None else f"{name} median throughput",
                args.y_label or "Throughput [operations/s]",
                timing_markers,
                restart_markers,
                median_output=args.median_output,
                baseline_series=baseline_series,
                restart_overlay_series=restart_overlay_series,
                primary_label=primary_label,
                restart_overlay_label=restart_overlay_label,
            )
            wrote_plot = True
        else:
            print(f"Skipping throughput for {name}: no plottable data")

    if "latency" in plots:
        latency_metrics = [
            ("avg_latency_us", "avg-latency", "average latency (us)"),
            ("p9999_latency_us", "p9999-latency", "p99.99 latency (us)"),
        ]
        for column, slug, label in latency_metrics:
            try:
                latency_series = collect_series(
                    run_groups,
                    args.x_column,
                    column,
                    restart_markers_by_run,
                    zero_restart_gap=False,
                )
            except ValueError as error:
                print(f"Skipping {label} for {name}: {error}")
                continue

            if not latency_series:
                continue

            plot_metric_set(
                latency_series,
                args,
                scenario_dir,
                output,
                "latency",
                slug,
                f"{name} {label}",
                f"{name} median {label}",
                label,
                timing_markers,
                restart_markers,
            )
            wrote_plot = True

    if "hit-rate" in plots:
        pgstats_run_groups = find_run_files(
            scenario_dir,
            glob_pattern="bench-run-*-pgstats.csv",
            run_file_re=PGSTATS_RUN_FILE_RE,
        )
        pgstats_run_groups = [
            group
            for group in pgstats_run_groups
            if group[0] not in excluded_runs
        ]
        if pgstats_run_groups:
            try:
                hit_rate_series = collect_series(
                    pgstats_run_groups,
                    args.x_column,
                    "buffer_hit_ratio_pct",
                    restart_markers_by_run,
                    zero_restart_gap=False,
                    run_file_re=PGSTATS_RUN_FILE_RE,
                )
            except ValueError as error:
                print(f"Skipping buffer hit rate for {name}: {error}")
            else:
                if hit_rate_series:
                    restart_overlay_hit_rate_series = None
                    if overlay_run_groups:
                        overlay_pgstats_run_groups = find_run_files(
                            args.restart_overlay_dir,
                            glob_pattern="bench-run-*-pgstats.csv",
                            run_file_re=PGSTATS_RUN_FILE_RE,
                        )
                        if not overlay_pgstats_run_groups:
                            raise SystemExit(
                                f"No bench-run-*-pgstats.csv files found in restart overlay directory {args.restart_overlay_dir}"
                            )
                        restart_overlay_hit_rate_series = collect_series(
                            overlay_pgstats_run_groups,
                            args.x_column,
                            "buffer_hit_ratio_pct",
                            overlay_restart_markers_by_run,
                            zero_restart_gap=False,
                            run_file_re=PGSTATS_RUN_FILE_RE,
                        )
                        if not restart_overlay_hit_rate_series:
                            raise SystemExit(
                                f"No plottable restart buffer hit-rate data found in {args.restart_overlay_dir}"
                            )
                    plot_metric_set(
                        hit_rate_series,
                        args,
                        scenario_dir,
                        output,
                        "hit-rate",
                        "hit-rate",
                        args.title if args.title is not None else f"{name} buffer hit rate",
                        args.title if args.title is not None else f"{name} median buffer hit rate",
                        args.y_label or "buffer hit rate (%)",
                        timing_markers,
                        restart_markers,
                        median_output=(
                            args.median_output
                            if plots == {"hit-rate"}
                            else None
                        ),
                        restart_overlay_series=restart_overlay_hit_rate_series,
                        restart_overlay_markers=overlay_restart_markers,
                        primary_label=primary_label,
                        restart_overlay_label=restart_overlay_label,
                        blank_restart_gap=True,
                    )
                    wrote_plot = True

    if "host-stat" in plots:
        try:
            host_series = collect_host_series(
                scenario_dir,
                args.host_column,
                args.timing_csv,
                args.host_divisor,
                excluded_runs,
            )
        except ValueError as error:
            print(f"Skipping host statistic for {name}: {error}")
        else:
            if host_series:
                host_slug = args.host_column.replace("_", "-")
                host_label = args.y_label or (
                    "Disk write rate [bytes/s]"
                    if args.host_column == "disk_write_bytes_per_s"
                    else args.host_column.replace("_", " ")
                )
                plot_metric_set(
                    host_series,
                    args,
                    scenario_dir,
                    output,
                    "host-stat",
                    host_slug,
                    args.title if args.title is not None else f"{name} {host_label}",
                    args.title if args.title is not None else f"{name} median {host_label}",
                    host_label,
                    timing_markers,
                    restart_markers,
                    median_output=(
                        args.median_output
                        if plots == {"host-stat"}
                        else None
                    ),
                )
                wrote_plot = True

    if not wrote_plot:
        print(f"Skipping {name}: no selected plots had plottable data")


def main():
    args = parse_args()

    if args.font_size is not None:
        plt.rcParams.update({"font.size": args.font_size})

    if args.batch is not None:
        scenarios = selected_scenario_dirs(
            args.batch,
            args.scenario,
            args.exclude_scenario,
        )
        if args.list_scenarios:
            for scenario_dir in scenarios:
                print(scenario_dir)
            return
        if not scenarios:
            raise SystemExit(f"No matching scenario directories found in {args.batch}")
        if args.output is not None:
            raise SystemExit("--output is only supported for single-scenario plotting; use --destination with --batch")
        if args.median_output is not None:
            raise SystemExit("--median-output is only supported for single-scenario plotting")

        for scenario_dir in scenarios:
            plot_scenario(args, scenario_dir, output_for_scenario(args, scenario_dir))
        return

    if args.list_scenarios:
        raise SystemExit("--list-scenarios requires --batch")
    if args.scenario or args.exclude_scenario:
        raise SystemExit("--scenario and --exclude-scenario require --batch")
    if args.destination is not None:
        raise SystemExit("--destination requires --batch")
    if args.scenario_dir is None:
        raise SystemExit("Provide a scenario directory or use --batch LOG_DIR")

    plot_scenario(args, args.scenario_dir, args.output)


if __name__ == "__main__":
    main()

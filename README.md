# Live Migration Benchmark Suite

Benchmark tooling and experimental data for the bachelor’s thesis:

> **A Case Study of Live Migration for Cloud Databases: Performance and Security Implications**

The project evaluates PostgreSQL workloads during QEMU/KVM live migration, cold restart, prewarmed restart, and streaming-replication failover.

## Getting started

The complete host setup, VM preparation, and benchmark instructions are documented in the [benchmark manual](manual.md).

The examples contain machine-specific absolute paths such as `/home/max/...`. Update these paths before running the benchmarks on another system.

## Benchmark scripts

| Script | Purpose |
|---|---|
| [`bench_migration_qmp.py`](Scripts/bench_migration_qmp.py) | Runs precopy, stop-and-copy, and postcopy migration benchmarks through QMP |
| [`bench_raw_qmp.py`](Scripts/bench_raw_qmp.py) | Runs baseline, cold-restart, prewarmed-restart, and prepared-standby benchmarks |
| [`bench_runner.sh`](Scripts/bench_runner.sh) | Coordinates the complete benchmark suite |
| [`event_logger.py`](Scripts/event_logger.py) | Records benchmark lifecycle events |
| [`host_resource_sampler.py`](Scripts/host_resource_sampler.py) | Samples host and QEMU resource usage |
| [`vm_postgres_script.sh`](Scripts/vm_postgres_script.sh) | Collects PostgreSQL buffer statistics inside the guest |

Run a script with `--help` to inspect its available options:

## Development history

The benchmark tooling evolved through three stages:

1. **QEMU/QMP utilities**  
   [`vm_launcher.sh`](Scripts/vm_launcher.sh), [`vm_status.sh`](Scripts/vm_status.sh), [`vm_query.sh`](Scripts/vm_query.sh), [`vm_migrate.sh`](Scripts/vm_migrate.sh), [`vm_shutdown.sh`](Scripts/vm_shutdown.sh), and [`launcher_base.sh`](Scripts/launcher_base.sh) were created while exploring QEMU startup, shutdown, migration, and QMP socket handling.

2. **Shell benchmark prototypes**  
   [`bench_mig_pgvm.sh`](Scripts/bench_mig_pgvm.sh) and [`bench_restart_pgvm.sh`](Scripts/bench_restart_pgvm.sh) combined those utilities into the first migration and restart benchmarks.

3. **Current Python implementation**  
   The shell prototypes were superseded by the Python benchmark scripts listed above, which provide argument parsing, automated repetitions, structured logging, resource sampling, and cleanup.

The older scripts are retained to document the development process but are not required to reproduce the final experiments.

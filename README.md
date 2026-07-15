# live-migration-thesis-code
This repository contains relevant data for the bachelor's thesis 
**"An Analysis of Live Migration and Its Security Implications"**.

### **scripts/** contains all main Shell and Python scripts used during my work on the thesis:
> [bench_migration_qmp.py](Scripts/bench_migration_qmp.py), [bench_raw_qmp.py](Scripts/bench_raw_qmp.py) are the main scripts to facilitate the benchmark.
> [bench_runner.sh](Scripts/bench_runner.sh) serves to coordinate the benchmark and run the above mentioned scripts.
> [event_logger.py](/Scripts/event_logger.py) and [host_resource_sampler.py](/Scripts/host_resource_sampler.py) serve to log events and useful hardware data during the benchmark runs, while [vm_postgres_script.sh](/Scripts/vm_postgres_script.sh) is run on the VM to extract PostgreSQL log data.
>
> [bench_mig_pgvm.sh](/Scripts/bench_mig_pgvm.sh) and [bench_restart_pgvm.sh](/Scripts/bench_restart_pgvm.sh) are the precursors to the now used python scripts. They were initially used to familiarize myself with QMP and the socket logic before the switch to Python.
> [vm_status.sh](/Scripts/vm_status.sh), [vm_query.sh](/Scripts/vm_query.sh), [vm_launcher.sh](/Scripts/vm_launcher.sh), [vm_shutdown.sh](/Scripts/vm_shutdown.sh), [vm_migrate.sh](/Scripts/vm_migrate.sh), [vm_launcher.sh](/Scripts/vm_launcher.sh), and [launcher_base.sh](/Scripts/launcher_base.sh) were created during the initial phase of the thesis work while learning the logic of QEMU/QMP.

### The current experimental state, results, logs, and plots can be found in [this directory](logs_14_major).
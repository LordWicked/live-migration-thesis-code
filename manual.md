The main scripts are:

- `bench_migration_qmp.py`: precopy, stop-and-copy, and postcopy live migration.
- `bench_raw_qmp.py`: baseline, cold restart, prewarmed restart, and prepared-standby restart.
- `bench_runner.sh`: runs the complete thesis benchmark suite.

## Host requirements

The host needs Linux with KVM, QEMU, `qemu-img`, SSH/SCP, and Python 3.

Create the Python environment from the repository root:

```bash
python3 -m venv .venv
.venv/bin/pip install qemu.qmp==0.0.6
```

Create the SSH key used by the scripts:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/pgvm_bench
```

All `/home/max/...` paths in the examples and in `Scripts/bench_runner.sh` are machine-specific absolute paths. Replace them when moving the benchmarks to another system.

## VM images

The benchmark uses three base images:

| Image | Purpose |
|---|---|
| `vm_test_qcow2` | Normal PostgreSQL VM used for migration, baseline, and cold restart |
| `prewarm_base_flat.qcow2` | Normal VM with `pg_prewarm` enabled |
| `pgstream_base_flat.qcow2` | PostgreSQL streaming-replication standby |

The Python scripts create disposable qcow2 overlays for each run. Do not use the overlay directory for unrelated images with names such as `run1.qcow2`.

## Opening and modifying an image

A base image can be started without a graphical display as follows:

```bash
qemu-system-x86_64 \
    -accel kvm \
    -m 8G \
    -smp 6 \
    -cpu host \
    -drive file=<image_directory>/<image_to_be_started>,if=virtio,format=qcow2,cache=none,aio=native \
    -nic user,hostfwd=tcp:127.0.0.1:2222-:22 \
    -qmp unix:/tmp/src-qmp.sock,server=on,wait=off \
    -display none
```
Modify -m, -smp depending on your system.

Connect to it with:

```bash
ssh -p 2222 user@127.0.0.1
```

Install the benchmark key during the initial setup:

```bash
ssh-copy-id -i ~/.ssh/pgvm_bench.pub -p 2222 user@127.0.0.1
```

Shut the guest down cleanly with `sudo poweroff` before copying or using its image as a benchmark base.

## Common guest setup

The commands below assume the Arch Linux PostgreSQL data directory `/var/lib/postgres/data`; adjust it for other distributions.

Install PostgreSQL, Java, Maven, and ACL support:

```bash
sudo pacman -S postgresql jdk-openjdk maven acl
```

The guest must contain:

- YCSB 0.17.0 at `/home/user/ycsb-0.17.0`
- The YCSB JDBC binding and PostgreSQL JDBC driver
- `db.properties` pointing to `jdbc:postgresql://127.0.0.1:5432/ycsb`
- The repository's `Scripts/vm_postgres_script.sh` copied to `/home/user/pg_sampler.sh`

Copy the sampler from the host:

```bash
scp -P 2222 Scripts/vm_postgres_script.sh \
    user@127.0.0.1:/home/user/pg_sampler.sh
```

Create the database and table:

```sql
CREATE ROLE ycsb LOGIN PASSWORD 'pass';
CREATE DATABASE ycsb OWNER ycsb;

\c ycsb

CREATE TABLE usertable (
    YCSB_KEY VARCHAR(255) PRIMARY KEY,
    FIELD0 TEXT, FIELD1 TEXT,
    FIELD2 TEXT, FIELD3 TEXT,
    FIELD4 TEXT, FIELD5 TEXT,
    FIELD6 TEXT, FIELD7 TEXT,
    FIELD8 TEXT, FIELD9 TEXT
);
```

Example `db.properties`:

```properties
db.driver=org.postgresql.Driver
db.url=jdbc:postgresql://127.0.0.1:5432/ycsb
db.user=ycsb
db.passwd=pass
```

Load the initial data (If recordcount is modified, this will need to be passed to the `bench_*.py` scripts using `--record-count`):

```bash
cd ~/ycsb-0.17.0
bin/ycsb.sh load jdbc \
    -P workloads/workloada \
    -P db.properties \
    -p recordcount=2500000
```

Then run `ANALYZE usertable;` and optionally `CHECKPOINT;`.

The scripts expect JSON PostgreSQL logs under `/var/log/postgresql`, as well as non-interactive `psql` access:

```text
user ALL=(postgres) NOPASSWD: /usr/bin/psql
```

Add this rule with:

```bash
sudo visudo -f /etc/sudoers.d/pgbench-postgres
sudo chmod 440 /etc/sudoers.d/pgbench-postgres
```

The PostgreSQL configuration used for the experiments included:

```ini
logging_collector = on
log_destination = 'jsonlog'
log_directory = '/var/log/postgresql'
log_filename = 'postgresql-%Y-%m-%d_%H%M%S.log'
log_min_messages = LOG
log_timezone = 'UTC'
log_startup_progress_interval = '1s'
log_checkpoints = on
log_line_prefix = '%m [%p] '
full_page_writes = off
listen_addresses = '*'
```

`full_page_writes = off` reproduces the experiment but is not recommended for production systems. Leaving it activated might provide other, also interesting, results.

## Creating the prewarmed image

With the normal base image powered off, make a flattened copy:

```bash
qemu-img convert -p -O qcow2 \
    <image_directory>/vm_test_qcow2 \
    <image_directory>/prewarm_base_flat.qcow2
```

Boot the copy and add:

```ini
shared_preload_libraries = 'pg_prewarm'
pg_prewarm.autoprewarm = true
pg_prewarm.autoprewarm_interval = 300s
```

Restart PostgreSQL and initialize the extension:

```sql
CREATE EXTENSION IF NOT EXISTS pg_prewarm;
SELECT autoprewarm_dump_now();
```

Then shut the VM down cleanly.

## Creating the streaming-standby image

Configure the normal VM for replication:

```sql
CREATE ROLE replicator
WITH REPLICATION LOGIN PASSWORD 'replicator_password';
```

Add to `pg_hba.conf`:

```text
host    all            all          0.0.0.0/0    md5
host    replication    replicator   0.0.0.0/0    md5
```

Create the destination image while the source is powered off:

```bash
qemu-img convert -p -O qcow2 \
    <image_directory>m/vm_test_qcow2 \
    <image_directory>/pgstream_base_flat.qcow2
```

Start the source with PostgreSQL forwarded through host port 5433:

```text
-nic user,hostfwd=tcp:127.0.0.1:2222-:22,hostfwd=tcp:127.0.0.1:5433-:5432
```

Start the destination image separately with SSH port 2223. Inside the destination:

```bash
sudo systemctl stop postgresql
sudo mv /var/lib/postgres/data /var/lib/postgres/data.old
sudo install -d -o postgres -g postgres -m 700 /var/lib/postgres/data

sudo -u postgres pg_basebackup \
    -h 10.0.2.2 \
    -p 5433 \
    -D /var/lib/postgres/data \
    -U replicator \
    -Fp -Xs -P -R

sudo systemctl start postgresql
```

Verify that it is a standby and receiving WAL:

```bash
sudo -u postgres psql -tA -c "SELECT pg_is_in_recovery();"
sudo -u postgres psql -x -c \
    "SELECT status, sender_host, sender_port, latest_end_lsn FROM pg_stat_wal_receiver;"
```

The first command must return `t`. Shut down both VMs cleanly when finished.

## Running the benchmarks

Run the scripts from `Scripts/`, because `bench_runner.sh` uses relative script paths:

```bash
cd Scripts
```

Example live migration:

```bash
../.venv/bin/python bench_migration_qmp.py \
    /home/max/Bachelor-Thesis/VMs/postgresvm/vm_test_qcow2 \
    /home/max/Bachelor-Thesis/VMs/postgresvm/bigbench \
    --runs 1 \
    --log-path ./logs/example-migration \
    --out-csv ./logs/example-migration/results.csv \
    --mem-gb 8 \
    --cores 4 \
    --migration-mode 0 \
    --operation-count 250000 \
    --threads 4
```

Migration modes are `0` for precopy, `1` for stop-and-copy, and `2` for postcopy.

Example cold restart:

```bash
../.venv/bin/python bench_raw_qmp.py \
    /home/max/Bachelor-Thesis/VMs/postgresvm/vm_test_qcow2 \
    /home/max/Bachelor-Thesis/VMs/postgresvm/bigbench \
    --restart 1 \
    --sleep-timer 150 \
    --runs 1
```

Additional restart scenarios:

```text
Prewarmed:
--restart 1 --prewarm 1 --prewarm-image /absolute/path/prewarm_base_flat.qcow2

Prepared standby:
--restart 1 --prepare-restart 1 --standby-image /absolute/path/pgstream_base_flat.qcow2
```

Use `--help` on either Python script for all workload, logging, CPU, memory, and SSH options.

To execute the complete thesis suite, update `RUNS`, `DIRECTORY`, and every absolute path in `bench_runner.sh`, then run:

```bash
./bench_runner.sh
```

Results are written to the configured log directory as CSV, JSON, QEMU logs, PostgreSQL logs, YCSB output, and host resource samples.
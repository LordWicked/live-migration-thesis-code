#!/usr/bin/bash

RUNS=1

mig_runner() {
    local tag=$1

    local memory=$2
    local sleep=$3
    local cores=$4
    local stop=$5
    local cpu=$6

    local opcount=$7
    local threads=$8
    local write=$9
    local read=${10}
    local upd=${11}
    local ins=${12}
    local rm=${13}
    local scan=${14}

    local logs="./logs_bigbench/$tag/${memory}G_2mil5-rec_${opcount}-ops_${sleep}-sleep_acoff_${threads}-thr" # TODO acoff
    mkdir -p "${logs}"

    echo -e "memory: ${memory}\nsleep: ${sleep}\ncores: ${cores}\ncpu: ${cpu}\nstop-copy: ${stop}\nopcount: ${opcount}\nthreads: ${threads}\nwrite-prop: ${write}\nread: ${read}\nupdate-prop: ${upd}\ninsert-prop: ${ins}\nreadmod-prop: ${rm}\nscan-prop: ${scan}" > "${logs}/${tag}_specs"

    ./bench_migration_qmp.py \
    /home/max/Bachelor-Thesis/VMs/postgresvm/vm_test_qcow2 \
    /home/max/Bachelor-Thesis/VMs/postgresvm/bigbench \
    --log-path "${logs}" \
    --out-csv "${logs}/${tag}.csv" \
    --runs "${RUNS}" \
    --mem-gb "${memory}" \
    --cores "${cores}" \
    --cpu "${cpu}" \
    --stop-copy "${stop}" \
    --operation-count "${opcount}" \
    --threads "${threads}" \
    --write-proportion "${write}" \
    --read-proportion "${read}" \
    --update-proportion "${upd}" \
    --insert-proportion "${ins}" \
    --readmodification-proportion "${rm}" \
    --scan-proportion "${scan}" \
    --sleep-timer "${sleep}"
}

restart_runner() {
    local tag=$1

    local memory=$2
    local sleep=$3
    local cores=$4
    local cpu=$5

    local opcount=$6
    local threads=$7
    local write=$8
    local read=$9
    local upd=${10}
    local ins=${11}
    local rm=${12}
    local scan=${13}
    local restart=${14}
    local prepare=${15}
    local prewarmed=${16}

    local logs="./logs_bigbench/$tag/${memory}G_2mil5-rec_${opcount}-ops_${sleep}-sleep_acoff_${threads}-thr"
    mkdir -p "${logs}"

    echo -e "memory: ${memory}\nsleep: ${sleep}\ncores: ${cores}\ncpu: ${cpu}\nstop-copy: ${stop}\nopcount: ${opcount}\nthreads: ${threads}\nwrite-prop: ${write}\nread: ${read}\nupdate-prop: ${upd}\ninsert-prop: ${ins}\nreadmod-prop: ${rm}\nscan-prop: ${scan}\nrestart: ${restart}\nprepare-VM2: ${prepare}\nprewarmed: ${prewarmed}" > "${logs}/${tag}_specs"

    ./bench_raw_qmp.py \
    /home/max/Bachelor-Thesis/VMs/postgresvm/vm_test_qcow2 \
    /home/max/Bachelor-Thesis/VMs/postgresvm/bigbench \
    --log-path "${logs}" \
    --out-csv "${logs}/${tag}.csv" \
    --runs "${RUNS}" \
    --mem-gb "${memory}" \
    --cores "${cores}" \
    --cpu "${cpu}" \
    --operation-count "${opcount}" \
    --threads "${threads}" \
    --write-proportion "${write}" \
    --read-proportion "${read}" \
    --update-proportion "${upd}" \
    --insert-proportion "${ins}" \
    --readmodification-proportion "${rm}" \
    --scan-proportion "${scan}" \
    --sleep-timer "${sleep}" \
    --restart "${restart}" \
    --prepare-restart "${prepare}"
}

# Raw
# restart_runner "restart_00" 8 0 4 host 1000000 16 1 0 0 0 0 0 False False False  # 8GB, 1 mil ops, 16 threads, 100% writes, 00 sec sleep, no restart

# Precopy 1: Bench faster than migration -> Migration finishes after bench
mig_runner "pre_10" 8 0 4 False host 1000000 16 1 0 0 0 0 0   # 8GB, 1 mil ops, 16 threads, 100% writes, 00 sec sleep
# mig_runner "pre_11" 8 5 4 False host 1000000 16 1 0 0 0 0 0   # 8GB, 1 mil ops, 16 threads, 100% writes, 05 sec sleep
# mig_runner "pre_12" 8 50 4 False host 1000000 8 1 0 0 0 0 0   # 8GB, 1 mil ops, 8 threads, 100% writes, 50 sec sleep
# mig_runner "pre_13" 8 50 4 False host 1000000 16 1 0 0 0 0 0  # 8GB, 1 mil ops, 16 threads, 100% writes, 50 sec sleep
# mig_runner "pre_14" 8 50 4 False host 1000000 32 1 0 0 0 0 0 # 8GB, 1 mil ops, 32 threads, 100% writes, 50 sec sleep

# # Precopy 2: Bench slower -> Migration happens quickly after call
# mig_runner "pre_20" 8 0 4 False host 100000 0 1 0 0 0 0 0    # 8GB, 100k ops, 0 threads, 100% writes, 00 sec sleep
# mig_runner "pre_21" 8 5 4 False host 100000 0 1 0 0 0 0 0    # 8GB, 100k ops, 0 threads, 100% writes, 05 sec sleep
# mig_runner "pre_22" 8 50 4 False host 100000 0 1 0 0 0 0 0   # 8GB, 100k ops, 0 threads, 100% writes, 50 sec sleep
# mig_runner "pre_23" 4 50 4 False host 100000 0 1 0 0 0 0 0   # 4GB, 100k ops, 0 threads, 100% writes, 50 sec sleep
# mig_runner "pre_24" 4 50 4 False host 100000 0 1 0 0 0 0 0   # 4GB, 100k ops, 0 threads, 100% writes, 50 sec sleep

# # Precopy 3: Immediate precopy -> immediate downtime and transition (QMP stop-and-copy)
# mig_runner "pre_30" 8 0 4 True host 100000 16 1 0 0 0 0 0   # 8GB, 50 ops, 16 threads, 100% writes, 00 sec sleep
# mig_runner "pre_31" 8 5 4 True host 100000 16 1 0 0 0 0 0   # 8GB, 50 ops, 16 threads, 100% writes, 05 sec sleep
# mig_runner "pre_32" 8 50 4 True host 100000 16 1 0 0 0 0 0  # 8GB, 50 ops, 16 threads, 100% writes, 50 sec sleep


# Postcopy: 



# Restart 1: Clean shutdown during benchmark (11:cold 12:prewarmed)
# restart_runner "restart_11" 8 50 4 host 1000000 16 1 0 0 0 0 0 True False False  # 8GB, 1 mil ops, 16 threads, 100% writes, 50 sec sleep
# restart_runner "restart_12" 8 50 4 host 1000000 16 1 0 0 0 0 0 True False True   # 8GB, 1 mil ops, 16 threads, 100% writes, 50 sec sleep, prewarmed

# # Restart 2: Clean shutdown with prepared restart (21:cold 22:prewarmed)
# restart_runner "restart_21" 8 50 4 host 1000000 16 1 0 0 0 0 0 True True False   # 8GB, 1 mil ops, 16 threads, 100% writes, 50 sec sleep
# restart_runner "restart_22" 8 50 4 host 1000000 16 1 0 0 0 0 0 True True True    # 8GB, 1 mil ops, 16 threads, 100% writes, 50 sec sleep, prewarmed


#!/usr/bin/bash

RUNS=1

mig_runner() {
    local tag=$1

    local memory=$2
    local sleep=$3
    local cores=$4
    local mode=$5
    local cpu=$6

    local opcount=$7
    local threads=$8
    local write=$9
    local read=${10}
    local upd=${11}
    local ins=${12}
    local rm=${13}
    local scan=${14}
    local ac=${15}

    local logs="./logs_bigbench/$tag/" #${memory}G_2mil5-rec_${opcount}-ops_${sleep}-sleep_ac${ac}_${threads}-thr # TODO acoff
    mkdir -p "${logs}"

    echo -e "memory: ${memory}\nsleep: ${sleep}\ncores: ${cores}\ncpu: ${cpu}\nmigration-mode (0: precopy, 1: stop-copy, 2: postcopy): ${mode}\nopcount: ${opcount}\nthreads: ${threads}\nwrite-prop: ${write}\nread: ${read}\nupdate-prop: ${upd}\ninsert-prop: ${ins}\nreadmod-prop: ${rm}\nscan-prop: ${scan}\nauto-converge: ${ac}" > "${logs}/${tag}_specs"

    ./bench_migration_qmp.py \
    /home/max/Bachelor-Thesis/VMs/postgresvm/vm_test_qcow2 \
    /home/max/Bachelor-Thesis/VMs/postgresvm/bigbench \
    --log-path "${logs}" \
    --out-csv "${logs}/${tag}.csv" \
    --runs "${RUNS}" \
    --mem-gb "${memory}" \
    --cores "${cores}" \
    --cpu "${cpu}" \
    --migration-mode "${mode}" \
    --operation-count "${opcount}" \
    --threads "${threads}" \
    --write-proportion "${write}" \
    --read-proportion "${read}" \
    --update-proportion "${upd}" \
    --insert-proportion "${ins}" \
    --readmodification-proportion "${rm}" \
    --scan-proportion "${scan}" \
    --sleep-timer "${sleep}" \
    --auto-converge "${ac}"
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
    local prewarmed=${15}
    # local prepare=${16}

    local logs="./logs_bigbench/$tag/" #${memory}G_2mil5-rec_${opcount}-ops_${sleep}-sleep_${threads}-thr
    mkdir -p "${logs}"

    echo -e "memory: ${memory}\nsleep: ${sleep}\ncores: ${cores}\ncpu: ${cpu}\nopcount: ${opcount}\nthreads: ${threads}\nwrite-prop: ${write}\nread: ${read}\nupdate-prop: ${upd}\ninsert-prop: ${ins}\nreadmod-prop: ${rm}\nscan-prop: ${scan}\nrestart: ${restart}\nprepare-VM2: ${prepare}\nprewarmed: ${prewarmed}" > "${logs}/${tag}_specs"

    ./bench_raw_qmp.py \
    /home/max/Bachelor-Thesis/VMs/postgresvm/vm_test_qcow2 \
    /home/max/Bachelor-Thesis/VMs/postgresvm/bigbench \
    --prewarm-image /home/max/Bachelor-Thesis/VMs/postgresvm/ \
    --log-path "${logs}" \
    --out-csv "${logs}/${tag}.csv" \
    --runs "${RUNS}" \
    --mem-gb "${memory}" \
    --sleep-timer "${sleep}" \
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
    --restart "${restart}" \
    # --prepare-restart "${prepare}"

    # TODO something to prewarm (extra image?)
}

# Raw # was 1000000
echo -e "raw:" && restart_runner "raw" 8 0 4 host 1000000 16 0 0 1 0 0 0 0 0  && echo       # 8GB, 1 mil ops, 16 threads, 100% writes, 00 sec sleep, no restart

# # Precopy 1: Bench faster than migration -> Migration finishes after bench
echo -e "pre_10:" && mig_runner "pre_10" 8 0 4 0 host 1000000 16 0 0 1 0 0 0 0 && echo      # 8GB, 1 mil ops, 16 threads, 100% writes, 00 sec sleep
echo -e "pre_11:" && mig_runner "pre_11"     8 5 4 0 host 1000000 16 0 0 1 0 0 0 0 && echo      # 8GB, 1 mil ops, 16 threads, 100% writes, 05 sec sleep
echo -e "pre_12:" && mig_runner "pre_12" 8 50 4 0 host 1000000 8 0 0 1 0 0 0 0 && echo      # 8GB, 1 mil ops, 8 threads, 100% writes, 50 sec sleep
echo -e "pre_13:" && mig_runner "pre_13" 8 50 4 0 host 1000000 16 0 0 1 0 0 0 0 && echo     # 8GB, 1 mil ops, 16 threads, 100% writes, 50 sec sleep
echo -e "pre_14:" && mig_runner "pre_14" 8 50 4 0 host 1000000 32 0 0 1 0 0 0 0 && echo     # 8GB, 1 mil ops, 32 threads, 100% writes, 50 sec sleep

# Precopy 2: Bench slower -> Migration happens quickly after call
echo -e "pre_20:" && mig_runner "pre_20" 8 0 4 0 host 100000 1 0 0 1 0 0 0 0 && echo        # 8GB, 100k ops, 0 threads, 100% writes, 00 sec sleep
echo -e "pre_21:" && mig_runner "pre_21" 8 5 4 0 host 100000 1 0 0 1 0 0 0 0 && echo        # 8GB, 100k ops, 0 threads, 100% writes, 05 sec sleep
echo -e "pre_22:" && mig_runner "pre_22" 8 50 4 0 host 100000 1 0 0 1 0 0 0 0 && echo       # 8GB, 100k ops, 0 threads, 100% writes, 50 sec sleep
echo -e "pre_23:" && mig_runner "pre_23" 4 50 4 0 host 100000 1 0 0 1 0 0 0 0 && echo       # 4GB, 100k ops, 0 threads, 100% writes, 50 sec sleep
echo -e "pre_24:" && mig_runner "pre_24" 4 50 4 0 host 100000 1 0 0 1 0 0 0 0 && echo       # 4GB, 100k ops, 0 threads, 100% writes, 50 sec sleep

# Precopy 3: Immediate precopy -> immediate downtime and transition (QMP stop-and-copy)
echo -e "pre_30:" && mig_runner "pre_30" 8 0 4 1 host 100000 16 0 0 1 0 0 0 0 && echo       # 8GB, 50 ops, 16 threads, 100% writes, 00 sec sleep
echo -e "pre_31:" && mig_runner "pre_31" 8 5 4 1 host 100000 16 0 0 1 0 0 0 0 && echo       # 8GB, 50 ops, 16 threads, 100% writes, 05 sec sleep
echo -e "pre_32:" && mig_runner "pre_32" 8 50 4 1 host 100000 16 0 0 1 0 0 0 0 && echo      # 8GB, 50 ops, 16 threads, 100% writes, 50 sec sleep


Postcopy: 
echo -e "post_10:" && mig_runner "post_10" 8 0 4 2 host 1000000 16 0 0 1 0 0 0 0 && echo    # 8GB, 1 mil ops, 16 threads, 100% writes, 00 sec sleep
echo -e "post_11:" && mig_runner "post_11" 8 5 4 2 host 1000000 16 0 0 1 0 0 0 0 && echo    # 8GB, 1 mil ops, 16 threads, 100% writes, 05 sec sleep
echo -e "post_12:" && mig_runner "post_12" 8 50 4 2 host 1000000 8 0 0 1 0 0 0 0 && echo    # 8GB, 1 mil ops, 8 threads, 100% writes, 50 sec sleep
echo -e "post_13:" && mig_runner "post_13" 8 50 4 2 host 1000000 16 0 0 1 0 0 0 0 && echo   # 8GB, 1 mil ops, 16 threads, 100% writes, 50 sec sleep

echo -e "post_20:" && mig_runner "post_20" 8 0 4 2 host 100000 1 0 0 1 0 0 0 0 && echo      # 8GB, 100k ops, 0 threads, 100% writes, 00 sec sleep
echo -e "post_21:" && mig_runner "post_21" 8 5 4 2 host 100000 1 0 0 1 0 0 0 0 && echo      # 8GB, 100k ops, 0 threads, 100% writes, 05 sec sleep
echo -e "post_22:" && mig_runner "post_22" 8 50 4 2 host 100000 1 0 0 1 0 0 0 0 && echo     # 8GB, 100k ops, 0 threads, 100% writes, 50 sec sleep
echo -e "post_23:" && mig_runner "post_23" 4 50 4 2 host 100000 1 0 0 1 0 0 0 0 && echo     # 4GB, 100k ops, 0 threads, 100% writes, 50 sec sleep
echo -e "post_24:" && mig_runner "post_24" 4 50 4 2 host 100000 1 0 0 1 0 0 0 0 && echo     # 4GB, 100k ops, 0 threads, 100% writes, 50 sec sleep

# Restart 1: Clean shutdown during benchmark (11:cold 12:prewarmed)
echo -e "restart_raw:" && restart_runner "restart_raw" 8 50 4 host 1000000 16 0 0 1 0 0 0 1 0 && echo  # 8GB, 1 mil ops, 16 threads, 100% writes, 50 sec sleep
echo -e "restart_prewarmed:" && restart_runner "restart_prewarmed" 8 50 4 host 1000000 16 0 0 1 0 0 0 1 1 && echo   # 8GB, 1 mil ops, 16 threads, 100% writes, 50 sec sleep, prewarmed

# # Maybe not easily doable:
# # Restart 2: Clean shutdown with prepared restart (21:cold 22:prewarmed)
# echo -e "restart_21:" && restart_runner "restart_21" 8 50 4 host 1000000 16 0 0 1 0 0 0 1 1 False && echo  # 8GB, 1 mil ops, 16 threads, 100% writes, 50 sec sleep
# restart_runner "restart_22" 8 50 4 host 1000000 16 0 0 1 0 0 0 True True True    # 8GB, 1 mil ops, 16 threads, 100% writes, 50 sec sleep, prewarmed


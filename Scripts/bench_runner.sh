#!/usr/bin/bash

RUNS=10
DIRECTORY="logs_0x_onetrialtorulethemall"
RECORDS=2500000

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
    local pcsleep=${16}

    local logs="./$DIRECTORY/$tag/"
    mkdir -p "${logs}"

    echo -e "memory: ${memory}\nsleep: ${sleep}\ncores: ${cores}\ncpu: ${cpu}\nmigration-mode (0: precopy, 1: stop-copy, 2: postcopy): ${mode}\nopcount: ${opcount}\nthreads: ${threads}\nwrite-prop: ${write}\nread: ${read}\nupdate-prop: ${upd}\ninsert-prop: ${ins}\nreadmod-prop: ${rm}\nscan-prop: ${scan}\nauto-converge: ${ac}\npostcopy-sleep after mig start: ${pcsleep}\n" > "${logs}/specs_${tag}"

    /home/max/Bachelor-Thesis/Repo/.venv/bin/python ./bench_migration_qmp.py \
    /home/max/Bachelor-Thesis/VMs/postgresvm/vm_test_qcow2 \
    /home/max/Bachelor-Thesis/VMs/postgresvm/bigbench \
    --log-path "${logs}" \
    --out-csv "${logs}/${tag}.csv" \
    --mem-gb "${memory}" \
    --cores "${cores}" \
    --cpu "${cpu}" \
    --migration-mode "${mode}" \
    --operation-count "${opcount}" \
    --record-count "${RECORDS}" \
    --threads "${threads}" \
    --write-proportion "${write}" \
    --read-proportion "${read}" \
    --update-proportion "${upd}" \
    --insert-proportion "${ins}" \
    --readmodification-proportion "${rm}" \
    --scan-proportion "${scan}" \
    --sleep-timer "${sleep}" \
    --auto-converge "${ac}" \
    --runs "${RUNS}" \
    --postcopy-sleep "${pcsleep}"
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
    local prepare=${16}

    local logs="./$DIRECTORY/$tag/"
    mkdir -p "${logs}"

    echo -e "memory: ${memory}\nsleep: ${sleep}\ncores: ${cores}\ncpu: ${cpu}\nopcount: ${opcount}\nthreads: ${threads}\nwrite-prop: ${write}\nread: ${read}\nupdate-prop: ${upd}\ninsert-prop: ${ins}\nreadmod-prop: ${rm}\nscan-prop: ${scan}\nrestart: ${restart}\nprewarmed: ${prewarmed}\nprepared: ${prepare}\n" > "${logs}/specs_${tag}"  # prepare-VM2: ${prepare}\n

    /home/max/Bachelor-Thesis/Repo/.venv/bin/python ./bench_raw_qmp.py \
    /home/max/Bachelor-Thesis/VMs/postgresvm/vm_test_qcow2 \
    /home/max/Bachelor-Thesis/VMs/postgresvm/bigbench \
    --prewarm-image /home/max/Bachelor-Thesis/VMs/postgresvm/prewarm_base_flat.qcow2 \
    --standby-image /home/max/Bachelor-Thesis/VMs/postgresvm/pgstream_base_flat.qcow2 \
    --log-path "${logs}" \
    --out-csv "${logs}/${tag}.csv" \
    --mem-gb "${memory}" \
    --sleep-timer "${sleep}" \
    --cores "${cores}" \
    --cpu "${cpu}" \
    --operation-count "${opcount}" \
    --record-count "${RECORDS}" \
    --threads "${threads}" \
    --write-proportion "${write}" \
    --read-proportion "${read}" \
    --update-proportion "${upd}" \
    --insert-proportion "${ins}" \
    --readmodification-proportion "${rm}" \
    --scan-proportion "${scan}" \
    --restart "${restart}" \
    --prepare-restart "${prepare}" \
    --prewarm "${prewarmed}" \
    --runs "${RUNS}"
}

# Raw # was 1000000
echo -e "Baseline_4T:" && restart_runner "Baseline_4T" 8 0 4 host 250000 4 0 0 1 0 0 0 0 0 0 && echo
echo -e "Baseline_16T:" && restart_runner "Baseline_16T" 8 0 4 host 500000 16 0 0 1 0 0 0 0 0 0 && echo
echo -e "Baseline_16T_Reads:" && restart_runner "Baseline_16T_Reads" 8 0 4 host 500000 16 0 0.9 0.1 0 0 0 0 0 0 && echo

# Precopy 1: Bench faster than migration -> Migration finishes after bench (/waren 1000000)
echo -e "Precopy_Convergent:" && mig_runner "Precopy_Convergent" 8 130 4 0 host 250000 4 0 0 1 0 0 0 0 0 && echo        # war 100s, genau auf dip

# Precopy 2: Bench slower -> Migration happens quickly after call (waren 100000)
echo -e "Precopy_Nonconvergent_16T:" && mig_runner "Precopy_Nonconvergent_16T" 8 100 4 0 host 500000 16 0 0 1 0 0 0 0 0 && echo        
echo -e "Precopy_Nonconvergent_Reads_16T:" && mig_runner "Precopy_Nonconvergent_Reads_16T" 8 100 4 0 host 500000 16 0 0.9 0.1 0 0 0 0 0 && echo  # run again

# Postcopy: # alle waren 1000000
# echo -e "post_light_1T:" && mig_runner "post_light_1T" 8 100 4 2 host 100000 1 0 0 1 0 0 0 0 0 && echo
echo -e "Postcopy_4T:" && mig_runner "Postcopy_4T" 8 130 4 2 host 250000 4 0 0 1 0 0 0 0 0 && echo                      # war 100s, genau auf dip
echo -e "Postcopy_16T:" && mig_runner "Postcopy_16T" 8 100 4 2 host 500000 16 0 0 1 0 0 0 0 0 && echo

# Precopy 3: Immediate precopy -> immediate downtime and transition (QMP stop-and-copy) waren 500000
echo -e "Stop_Copy:" && mig_runner "Stop_Copy_4T" 8 130 4 1 host 250000 4 0 0 1 0 0 0 0 0 && echo

# Precopy Autoconverge:
echo -e "Autoconverge_16T:" && mig_runner "Autoconverge_16T" 8 100 4 0 host 500000 16 0 0 1 0 0 0 1 0 && echo

# Belated Postcopy:
echo -e "Postcopy_Late_16T_25s:" && mig_runner "Postcopy_Late_16T_25s" 8 100 4 2 host 500000 16 0 0 1 0 0 0 0 25 && echo
echo -e "Postcopy_Late_16T_40s:" && mig_runner "Postcopy_Late_16T_40s" 8 100 4 2 host 500000 16 0 0 1 0 0 0 0 40 && echo

# # Restart 1: Clean shutdown during benchmark (11:cold 12:prewarmed)
echo -e "Cold_Restart_4T:" && restart_runner "Cold_Restart_4T" 8 130 4 host 250000 4 0 0 1 0 0 0 1 0 0 && echo
echo -e "Prewarmed_4T:" && restart_runner "Prewarmed_4T" 8 130 4 host 250000 4 0 0 1 0 0 0 1 1 0 && echo

# # Restart 2: Clean shutdown with prepared restart (21:cold 22:prewarmed)
echo -e "Prepared_4T:" && restart_runner "Prepared_4T" 8 130 4 host 250000 4 0 0 1 0 0 0 1 0 1 && echo




# Lost Benchmarks:
# echo -e "raw_clean_1T:" && restart_runner "raw_clean_1T" 8 0 4 host 100000 1 0 0 1 0 0 0 0 0 0 && echo
# echo -e "pre_light_convergent:" && mig_runner "pre_light_convergent" 8 100 4 0 host 100000 1 0 0 1 0 0 0 0 0 && echo
# echo -e "post_late_12T:" && mig_runner "spam_betterstatus_pretopost_late_50s_12T" 8 100 4 2 host 500000 12 0 0 1 0 0 0 0 50 && echo
# echo -e "pre_nonconvergent_12T:" && mig_runner "spam_pre_nonconvergent_12T" 8 100 4 0 host 500000 12 0 0 1 0 0 0 0 0 && echo        
# echo -e "raw:" && restart_runner "raw_stress_8T" 8 0 4 host 500000 8 0 0 1 0 0 0 0 0 0 && echo       # 8GB, 100k ops, 1 threads, 100% writes, 00 sec sleep, no restart
# echo -e "raw:" && restart_runner "raw_stress_16T" 8 0 4 host 500000 16 0 0 1 0 0 0 0 0 0 && echo       # 8GB, 100k ops, 1 threads, 100% writes, 00 sec sleep, no restart

# echo -e "pre_11:" && mig_runner "pre_stress_8T_1500guest" 8 100 4 0 host 500000 8 0 0 1 0 0 0 0 && echo      # 8GB, 100k ops, 1 threads, 100% writes, 05 sec sleep
# echo -e "pre_11:" && mig_runner "pre_stress_12T" 8 100 4 0 host 500000 12 0 0 1 0 0 0 0 && echo      # 8GB, 100k ops, 1 threads, 100% writes, 05 sec sleep
# echo -e "pre_11:" && mig_runner "pre_stress_16T" 8 100 4 0 host 500000 16 0 0 1 0 0 0 0 && echo      # 8GB, 100k ops, 1 threads, 100% writes, 05 sec sleep

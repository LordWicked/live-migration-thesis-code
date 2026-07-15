> The relevant commands to produce these thesis-relevant plots.
> The whole directory was in /Scripts before.

# Baseline
## Throughput
### 4 Threads
Scripts/plot_throughput.py \
  Scripts/logs_14_major/Baseline_4T \
  --plots throughput \
  --omit-incomplete-median-tail \
  --title '' \
  --x-label 'Elapsed time [s]' \
  --y-label 'Throughput [operations/s]' \
  --font-size 14 \
  --x-range 0 310 \
  --y-range 0 2500 \
  --x-tick-step 25 \
  --output /tmp/Baseline_4T-throughput.pdf \
  --median-output Scripts/logs_14_major/plots/Baseline_4T-throughput-median.pdf
### 16 Threads
Scripts/plot_throughput.py \
  Scripts/logs_14_major/Precopy_Nonconvergent_16T \
  --plots throughput \
  --baseline-dir Scripts/logs_14_major/Baseline_16T \
  --no-baseline-iqr \
  --omit-incomplete-median-tail \
  --title '' \
  --x-label 'Elapsed time [s]' \
  --y-label 'Throughput [operations/s]' \
  --font-size 14 \
  --x-range 0 210 \
  --y-range 0 7000 \
  --x-tick-step 25 \
  --output /tmp/Precopy_Nonconvergent_16T-throughput.pdf \
  --median-output Scripts/logs_14_major/plots/Precopy_Nonconvergent_16T-throughput-median.pdf
# Main
## Throughput
### Precopy
Scripts/plot_throughput.py \
  Scripts/logs_14_major/Precopy_Convergent \
  --plots throughput \
  --baseline-dir Scripts/logs_14_major/Baseline_4T \
  --no-baseline-iqr \
  --omit-incomplete-median-tail \
  --title '' \
  --x-label 'Elapsed time [s]' \
  --y-label 'Throughput [operations/s]' \
  --font-size 14 \
  --x-range 0 330 \
  --y-range 0 2500 \
  --x-tick-step 25 \
  --output /tmp/Precopy_Convergent-throughput.pdf \
  --median-output Scripts/logs_14_major/plots/Precopy_Convergent-throughput-median.pdf
### Postcopy
Scripts/plot_throughput.py \
  Scripts/logs_14_major/Postcopy_4T \
  --plots throughput \
  --baseline-dir Scripts/logs_14_major/Baseline_4T \
  --no-baseline-iqr \
  --omit-incomplete-median-tail \
  --title '' \
  --x-label 'Elapsed time [s]' \
  --y-label 'Throughput [operations/s]' \
  --font-size 14 \
  --x-range 0 330 \
  --y-range 0 2750 \
  --x-tick-step 25 \
  --output /tmp/Postcopy_4T-throughput.pdf \
  --median-output Scripts/logs_14_major/plots/Postcopy_4T-throughput-median.pdf
### Stop-Copy
Scripts/plot_throughput.py \
  Scripts/logs_14_major/Stop_Copy_4T \
  --plots throughput \
  --baseline-dir Scripts/logs_14_major/Baseline_4T \
  --no-baseline-iqr \
  --omit-incomplete-median-tail \
  --title '' \
  --x-label 'Elapsed time [s]' \
  --y-label 'Throughput [operations/s]' \
  --font-size 14 \
  --x-range 0 330 \
  --y-range 0 2750 \
  --x-tick-step 25 \
  --output /tmp/Stop_Copy_4T-throughput.pdf \
  --median-output Scripts/logs_14_major/plots/Stop_Copy_4T-throughput-median.pdf
# Stress
## Throughput
### Nonconvergent Precopy
Scripts/plot_throughput.py \
  Scripts/logs_14_major/Precopy_Nonconvergent_16T \
  --plots throughput \
  --baseline-dir Scripts/logs_14_major/Baseline_16T \
  --no-baseline-iqr \
  --omit-incomplete-median-tail \
  --title '' \
  --x-label 'Elapsed time [s]' \
  --y-label 'Throughput [operations/s]' \
  --font-size 14 \
  --x-range 0 225 \
  --y-range 0 7200 \
  --x-tick-step 25 \
  --output /tmp/Precopy_Nonconvergent_16T-throughput.pdf \
  --median-output Scripts/logs_14_major/plots/Precopy_Nonconvergent_16T-throughput-median.pdf
### Postcopy
Scripts/plot_throughput.py \
  Scripts/logs_14_major/Postcopy_16T \
  --plots throughput \
  --baseline-dir Scripts/logs_14_major/Baseline_16T \
  --no-baseline-iqr \
  --omit-incomplete-median-tail \
  --title '' \
  --x-label 'Elapsed time [s]' \
  --y-label 'Throughput [operations/s]' \
  --font-size 14 \
  --x-range 0 225 \
  --y-range 0 7200 \
  --x-tick-step 25 \
  --output /tmp/Postcopy_16T-throughput.pdf \
  --median-output Scripts/logs_14_major/plots/Postcopy_16T-throughput-median.pdf
# Hybrid
## Throughput
### Autoconverge
Scripts/plot_throughput.py \
  Scripts/logs_14_major/Autoconverge_16T \
  --plots throughput \
  --baseline-dir Scripts/logs_14_major/Baseline_16T \
  --no-baseline-iqr \
  --omit-incomplete-median-tail \
  --title '' \
  --x-label 'Elapsed time [s]' \
  --y-label 'Throughput [operations/s]' \
  --font-size 14 \
  --x-range 0 210 \
  --y-range 0 7000 \
  --x-tick-step 25 \
  --output /tmp/Hybrid-Autoconverge_16T-throughput.pdf \
  --median-output Scripts/logs_14_major/plots/Hybrid-Autoconverge_16T-throughput-median.pdf
### 5s
Scripts/plot_throughput.py \
  Scripts/logs_14_major/Postcopy_Late_16T_5s \
  --plots throughput \
  --baseline-dir Scripts/logs_14_major/Baseline_16T \
  --no-baseline-iqr \
  --omit-incomplete-median-tail \
  --title '' \
  --x-label 'Elapsed time [s]' \
  --y-label 'Throughput [operations/s]' \
  --font-size 14 \
  --x-range 0 210 \
  --y-range 0 7000 \
  --x-tick-step 25 \
  --output /tmp/Hybrid-Postcopy_Late_16T_5s-throughput.pdf \
  --median-output Scripts/logs_14_major/plots/Hybrid-Postcopy_Late_16T_5s-throughput-median.pdf
### 25s
Scripts/plot_throughput.py \
  Scripts/logs_14_major/Postcopy_Late_16T_25s \
  --plots throughput \
  --baseline-dir Scripts/logs_14_major/Baseline_16T \
  --no-baseline-iqr \
  --omit-incomplete-median-tail \
  --title '' \
  --x-label 'Elapsed time [s]' \
  --y-label 'Throughput [operations/s]' \
  --font-size 14 \
  --x-range 0 210 \
  --y-range 0 7000 \
  --x-tick-step 25 \
  --output /tmp/Hybrid-Postcopy_Late_16T_25s-throughput.pdf \
  --median-output Scripts/logs_14_major/plots/Hybrid-Postcopy_Late_16T_25s-throughput-median.pdf
# Cold vs. Prewarmed
### Throughput
Scripts/plot_throughput.py \
  Scripts/logs_14_major/Prepared_4T \
  --plots throughput \
  --restart-overlay-dir Scripts/logs_14_major/Cold_Restart_4T \
  --primary-label Prepared \
  --restart-overlay-label Cold \
  --zero-restart-gap \
  --omit-incomplete-median-tail \
  --title '' \
  --x-label 'Elapsed time [s]' \
  --y-label 'Throughput [operations/s]' \
  --font-size 14 \
  --x-range 0 512 \
  --y-range 0 2500 \
  --x-tick-step 25 \
  --output /tmp/Cold-vs-Prepared-Restart-throughput.pdf \
  --median-output Scripts/logs_14_major/plots/Cold-vs-Prepared-Restart-throughput-median.pdf
### Hit-Rate
Scripts/plot_throughput.py \
  Scripts/logs_14_major/Prewarmed_4T \
  --plots hit-rate \
  --restart-overlay-dir Scripts/logs_14_major/Cold_Restart_4T \
  --primary-label Prewarmed \
  --restart-overlay-label Cold \
  --omit-incomplete-median-tail \
  --title '' \
  --x-label 'Elapsed time [s]' \
  --y-label 'Buffer hit rate [%]' \
  --font-size 14 \
  --x-range 0 361 \
  --y-range 75 100 \
  --x-tick-step 25 \
  --output /tmp/Cold-vs-Prewarmed-Restart-hit-rate.pdf \
  --median-output Scripts/logs_14_major/plots/Cold-vs-Prewarmed-Restart-hit-rate-median.pdf
# Prepared Switchover
### Throughput
Scripts/plot_throughput.py \
  Scripts/logs_14_major/Prepared_4T \
  --plots throughput \
  --exclude-run 1 \
  --omit-incomplete-median-tail \
  --min-median-runs 8 \
  --title '' \
  --x-label 'Elapsed time [s]' \
  --y-label 'Throughput [operations/s]' \
  --font-size 14 \
  --x-range 0 553 \
  --y-range 0 2500 \
  --x-tick-step 25 \
  --output /tmp/Prepared-Restart-throughput.pdf \
  --median-output Scripts/logs_14_major/plots/Prepared-throughput-median.pdf
### I/O PSI
Scripts/plot_throughput.py \
  Scripts/logs_14_major/Prepared_4T \
  --plots host-stat \
  --host-column psi_io_full_avg10 \
  --exclude-run 1 \
  --omit-incomplete-median-tail \
  --min-median-runs 8 \
  --title '' \
  --x-label 'Elapsed time [s]' \
  --y-label 'Full I/O pressure [%]' \
  --font-size 14 \
  --x-range 0 553 \
  --y-range 0 16 \
  --x-tick-step 25 \
  --output /tmp/Prepared-iopressure.pdf \
  --median-output Scripts/logs_14_major/plots/Prepared-iopressure-median.pdf
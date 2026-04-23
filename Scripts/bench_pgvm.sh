#!/usr/bin/bash
set -euo pipefail

RUNS="${RUNS:-10}"
IMAGE="${1:?Image path}"
SSH_PORT_BASE="${SSH_PORT_BASE:-2222}"
PG_PORT_BASE="${PG_PORT_BASE:-54320}"
GUEST_USER="user"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/pgvm_bench}"
OUT_CSV="${OUT_CSV:-boot-benchmark.csv}"

echo "run, shh_ready_ms, pg_ready_ms" > "$OUT_CSV"

for run in $(seq 1 "$RUNS"); do
	ssh_port=$((SSH_PORT_BASE + run - 1))
	pg_port=$((PG_PORT_BASE + run - 1))
	qmp_sock=$"/tmp/pgvm-qmp-${run}.sock"
	qemu_log="qemu-run-${run}.log"

	rm -f "$qmp_sock" "$qemu_log"

	start_ns=$(date +%s%N)

	qemu-system-x86_64 \
		-accel kvm \
		-m 8G \
		-smp 4 \
		-cpu host \
		-snapshot \
		-drive file="$IMAGE",if=virtio,format=qcow2 \
		-nic user,hostfwd=tcp:127.0.0.1:${ssh_port}-:22 \
		-qmp unix:"$qmp_sock",server=on,wait=off \
		-display none \
		>"$qemu_log" 2>&1 &

	qemu_pid=$!

	cleanup() {
		kill "$qemu_pid" 2>/dev/null || true
		wait "$qemu_pid" 2>/dev/null || true
		rm -f "$qmp_sock"
	}
	trap cleanup EXIT

#	echo "ssh -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=1 -p "$ssh_port" "$GUEST_USER@127.0.0.1""
#	sleep 10
	until ssh -i "$SSH_KEY" \
		-o BatchMode=yes \
		-o StrictHostKeyChecking=no \
		-o UserKnownHostsFile=/dev/null \
		-o ConnectTimeout=1 \
		-p "$ssh_port" \
		"$GUEST_USER@127.0.0.1" true > /dev/null 2>&1
	do
		if ! kill -0 "$qemu_pid" 2>/dev/null; then
			echo "QEMU exited unexpectedly in run $run" >&2
			exit 1
		fi
		sleep 0.2
	done
	ssh_ready_ns=$(date +%s%N)
	ctr=0
	until pg_isready -h 127.0.0.1 -p "$pg_port" -q -t 1 >/dev/null 2>&1
	do
		if ! kill -0 "$qemu_pid" 2>/dev/null; then
			echo "QEMU exited unexpectedly in run $run" >&2
			exit 1
		fi
		echo "$ctr"
		ctr=$((ctr + 1))
		sleep 0.2
	done
	pg_ready_ns=$(date +%s%N)

	ssh_ready_ms=$(( (ssh_ready_ns - start_ns) / 1000000 ))
	pg_ready_ms=$(( (pg_ready_ns - start_ns) / 1000000 ))

	echo "$run,$ssh_ready_ms,$pg_ready_ms" >> "$OUT_CSV"
	echo "run $run: ssh=${ssh_ready_ms} ms, postgres=${pg_ready_ms} ms"

	cleanup
	trap - EXIT

	sleep 2
done
echo
echo "Results written to $OUT_CSV"

#!/usr/bin/bash
set -euo pipefail

RUNS="${RUNS:-10}"
IMAGE="${1:?Image path}"
SSH_PORT_BASE="${SSH_PORT_BASE:-2222}"
PG_PORT_BASE="${PG_PORT_BASE:-54320}"
GUEST_USER="user"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/pgvm_bench}"
OUT_CSV="${OUT_CSV:-migration-benchmark.csv}"
MEM="${MEM:-8}"

echo "run, ssh_ready_ms, pg_ready_ms, ssh2_ready_ms, pg2_ready_ms" > "$OUT_CSV"

for run in $(seq 1 "$RUNS"); do
	ssh_port=$((SSH_PORT_BASE + run - 1))
	pg_port=$((PG_PORT_BASE + run - 1))
	src_sock=$"/tmp/src-pgvm-qmp-${run}.sock"
	dst_sock=$"/tmp/dst-pgvm-qmp-${run}.sock"
    qemu_log="qemu-run-${run}.log"
    overlay="$IMAGE/runs/run$run"

    rem -f "$qmp_sock" "$qemu_log"

    # Create Benchmark overlay image
    qemu-img create -f qcow2 -b "$IMAGE" overlay

    start_ns=$(date +%s%N) #before or after overlay creation?

    # Start source VM
    qemu-system-x86_64 \
        -accel kvm \
        -m "${MEM}"G \
		-smp 4 \
		-cpu host \
		-drive file="$overlay",if=virtio,format=qcow2 \
		-nic user,hostfwd=tcp:127.0.0.1:${ssh_port}-:22 \
		-qmp unix:"$src_sock",server=on,wait=off \
		-display none \
		>"$qemu_log" 2>&1 &

    src_pid=$!

    # Start destination VM
    qemu-system-x86_64 \
        -accel kvm \
        -m "${MEM}"G \
		-smp 4 \
		-cpu host \
		-drive file="$overlay",if=virtio,format=qcow2 \
		-nic user,hostfwd=tcp:127.0.0.2:${ssh_port}-:22 \
		-qmp unix:"$dst_sock",server=on,wait=off \
        -incoming unix:"$MIG_SOCK" \
		-display none \
		>"$qemu_log" 2>&1 &

    dst_pid=$!

    cleanup() {
        kill "$src_pid" 2>/dev/null || true
        wait "$src_pid" 2>/dev/null || true
        kill "$dst_pid" 2>/dev/null || true
        wait "$dst_pid" 2>/dev/null || true
        rm -f "$src_sock"
        rm -f "$dst_sock"
    }
    trap cleanup EXIT

    # Try source SSH
    until ssh -i "$SSH_KEY" \
		-o BatchMode=yes \
		-o StrictHostKeyChecking=no \
		-o UserKnownHostsFile=/dev/null \
		-o ConnectTimeout=1 \
		-p "$ssh_port" \
		"$GUEST_USER@127.0.0.1" true > /dev/null 2>&1
	do
		if ! kill -0 "$dst_pid" 2>/dev/null; then
			echo "QEMU exited unexpectedly in run $run" >&2
			exit 1
		fi
		sleep 0.1
	done
	ssh_ready_ns=$(date +%s%N)

    # Try source pg_isready via SSH
	until ssh -i "$SSH_KEY" \
    	-o BatchMode=yes \
    	-o StrictHostKeyChecking=no \
    	-o UserKnownHostsFile=/dev/null \
    	-o ConnectTimeout=1 \
    	-p "$ssh_port" \
    	"$GUEST_USER@127.0.0.1" \
    	'pg_isready -h 127.0.0.1 -p 5432 -q -t 1' >/dev/null 2>&1
	do
		if ! kill -0 "$qemu_pid" 2>/dev/null; then
			echo "QEMU exited unexpectedly before PostgreSQL was ready in run $run" >&2
			exit 1
		fi
		sleep 0.1
	done
	pg_ready_ns=$(date +%s%N)

    # TODO Start PostgreSQL bench or something via SSH here

    # Start Live Migration
    printf '%s\n' \
        '{"execute":"qmp_capabilities"}' \
        '{"execute":"migrate","arguments":{"uri":"unix:/tmp/mig.sock"}}' \
    | socat - UNIX-CONNECT:${src_sock}

    # Try destination SSH
    until ssh -i "$SSH_KEY" \
		-o BatchMode=yes \
		-o StrictHostKeyChecking=no \
		-o UserKnownHostsFile=/dev/null \
		-o ConnectTimeout=1 \
		-p "$ssh_port" \
		"$GUEST_USER@127.0.0.2" true > /dev/null 2>&1
	do
		if ! kill -0 "$qemu_pid" 2>/dev/null; then
			echo "QEMU exited unexpectedly in run $run on the destination VM" >&2
			exit 1
		fi
		sleep 0.1
	done

	ssh2_ready_ns=$(date +%s%N)
    
    # Try destination pg_isready via SSH
	until ssh -i "$SSH_KEY" \
    	-o BatchMode=yes \
    	-o StrictHostKeyChecking=no \
    	-o UserKnownHostsFile=/dev/null \
    	-o ConnectTimeout=1 \
    	-p "$ssh_port" \
    	"$GUEST_USER@127.0.0.2" \
    	'pg_isready -h 127.0.0.2 -p 5432 -q -t 1' #>/dev/null 2>&1
	do
		if ! kill -0 "$qemu_pid" 2>/dev/null; then
			echo "QEMU exited unexpectedly before destination PostgreSQL was ready in run $run" >&2
			exit 1
		fi
		# echo "$ctr"
		# ctr=$((ctr + 1))
		sleep 0.1
	done
    
    pg2_ready_ns=$(date +%s%N)

    # TODO Measure until bench is finished

    ssh_ready_ms=$(( (ssh_ready_ns - start_ns) / 1000000 ))
	pg_ready_ms=$(( (pg_ready_ns - start_ns) / 1000000 ))
    ssh2_ready_ms=$(( (ssh2_ready_ns - start_ns) / 1000000 ))
	pg2_ready_ms=$(( (pg2_ready_ns - start_ns) / 1000000 ))

    echo "$run,$ssh_ready_ms,$pg_ready_ms,$ssh2_ready_ms,$pg2_ready_ms" >> "$OUT_CSV"
	echo "run $run: ssh=${ssh_ready_ms} ms, postgres=${pg_ready_ms} ms, ssh_dest=${ssh2_ready_ms}, postgres_dest=${pg2_ready_ms}"


    printf '%s\n' \
        '{"execute":"qmp_capabilities"}' \
        '{"execute":"system_powerdown"}' \
    | socat - UNIX-CONNECT:${dst_sock}

    printf '%s\n' \
        '{"execute":"qmp_capabilities"}' \
        '{"execute":"system_powerdown"}' \
    | socat - UNIX-CONNECT:${src_sock}

    cleanup
    trap - EXIT

    sleep
done
echo
echo "Results written to $OUT_CSV"
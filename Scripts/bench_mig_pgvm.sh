#!/usr/bin/bash
set -euo pipefail

RUNS="${RUNS:-10}"
IMAGE="${1:?Image path}"
OVERLAY_DIR="${2:?Overlay file path}"
LOGS="${LOGS:-./logs}"
SRC_PORT_BASE="${SSH_PORT_BASE:-2222}"
DST_PORT_BASE="${SSH_PORT_BASE:-4444}"
GUEST_USER="user"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/pgvm_bench}"
OUT_CSV="${OUT_CSV:-migration-benchmark.csv}"
MEM="${MEM:-4}"

mkdir -p "$OVERLAY_DIR"
mkdir -p "$LOGS"
echo "run,ssh_ready_ms,pg_ready_ms,mig_done_ms,ssh2_ready_ms,pg2_ready_ms" > "$OUT_CSV"


for run in $(seq 1 "$RUNS"); do
	src_port=$((SRC_PORT_BASE + run - 1))
	dst_port=$((DST_PORT_BASE + run - 1))
	src_sock=$"/tmp/src-pgvm-qmp-${run}.sock"
	dst_sock=$"/tmp/dst-pgvm-qmp-${run}.sock"
    src_log="$LOGS/src-run-${run}.log"
	dst_log="$LOGS/dst-run-${run}.log"
    overlay="$OVERLAY_DIR/run$run"

	rm -f /tmp/mig.sock "$src_sock" "$dst_sock" "$overlay"

	rm -f "$src_sock" "$dst_sock" "$src_log" "$dst_log"

    # Create Benchmark overlay image
    qemu-img create -o "backing_file=$IMAGE,backing_fmt=qcow2" -f qcow2 "$overlay" > /dev/null 2>&1

    start_ns=$(date +%s%N) #before or after overlay creation?

    # Start source VM
    qemu-system-x86_64 \
        -accel kvm \
        -m "${MEM}G" \
		-smp 4 \
		-cpu host \
		-drive file="$overlay",if=virtio,format=qcow2 \
		-nic user,hostfwd=tcp:127.0.0.1:${src_port}-:22 \
		-qmp unix:"$src_sock",server=on,wait=off \
		-display none \
		>"$src_log" 2>&1 &

    src_pid=$!

    # Start destination VM
    qemu-system-x86_64 \
        -accel kvm \
        -m "${MEM}G" \
		-smp 4 \
		-cpu host \
		-drive file="$overlay",if=virtio,format=qcow2 \
		-nic user,hostfwd=tcp:127.0.0.1:${dst_port}-:22 \
		-qmp unix:"$dst_sock",server=on,wait=off \
        -incoming unix:/tmp/mig.sock \
		-display none \
		>"$dst_log" 2>&1 &

    dst_pid=$!

	# Functions
    cleanup() {
        kill "$src_pid" 2>/dev/null || true
        wait "$src_pid" 2>/dev/null || true
        kill "$dst_pid" 2>/dev/null || true
        wait "$dst_pid" 2>/dev/null || true
        rm -f "$src_sock"
        rm -f "$dst_sock"
    }
    trap cleanup EXIT

	qmp_cmd() {
		local sock="$1"
		local execute="$2"
		local args="${3:-{}}"

		{
			printf '%s\n' '{"execute":"qmp_capabilities","id":"cap"}'
			printf '{"execute":"%s","arguments":%s,"id":"cmd"}\n' "$execute" "$args"
		} | socat - UNIX-CONNECT:"$sock" | jq -c 'select(.id=="cmd")'
	}

	wait_for_migration_complete() {
		local sock="$1"
		local deadline=$((SECONDS + 120))
		local reply status

		while :; do
			reply="$(qmp_cmd "$sock" query-migrate)"
			printf '%s\n' "$reply" > "${LOGS}/mig-stats-run${run}.json"
			status="$(jq -r '.return.status // "none"' <<<"$reply")"
			case "$status" in
				completed) return 0 ;;
				failed|failing|cancelled) echo "$reply" >&2; return 1 ;;
			esac

			(( SECONDS >= deadline )) && return 1
			sleep 0.1
		done
	}

    # Try source SSH
    until ssh -i "$SSH_KEY" \
		-o BatchMode=yes \
		-o StrictHostKeyChecking=no \
		-o UserKnownHostsFile=/dev/null \
		-o ConnectTimeout=1 \
		-p "$src_port" \
		"$GUEST_USER@127.0.0.1" true > /dev/null 2>&1
	do
		if ! kill -0 "$src_pid" 2>/dev/null; then
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
    	-p "$src_port" \
    	"$GUEST_USER@127.0.0.1" \
    	'pg_isready -h 127.0.0.1 -p 5432 -q -t 1' >/dev/null 2>&1
	do
		if ! kill -0 "$src_pid" 2>/dev/null; then
			echo "QEMU exited unexpectedly before PostgreSQL was ready in run $run" >&2
			exit 1
		fi
		sleep 0.1
	done
	pg_ready_ns=$(date +%s%N)

    # TODO Start PostgreSQL bench or something via SSH here

	qmp_cmd "$src_sock" migrate '{"uri":"unix:/tmp/mig.sock"'

	wait_for_migration_complete "$src_sock"
	mig_done_ns=$(date +%s%N)

    # Try destination SSH
    until ssh -i "$SSH_KEY" \
		-o BatchMode=yes \
		-o StrictHostKeyChecking=no \
		-o UserKnownHostsFile=/dev/null \
		-o ConnectTimeout=1 \
		-p "$dst_port" \
		"$GUEST_USER@127.0.0.1" true > /dev/null 2>&1
	do
		if ! kill -0 "$dst_pid" 2>/dev/null; then
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
    	-p "$dst_port" \
    	"$GUEST_USER@127.0.0.1" \
    	'pg_isready -h 127.0.0.1 -p 5432 -q -t 1' >/dev/null 2>&1
	do
		if ! kill -0 "$dst_pid" 2>/dev/null; then
			echo "QEMU exited unexpectedly before destination PostgreSQL was ready in run $run" >&2
			exit 1
		fi
		sleep 0.1
	done
    
    pg2_ready_ns=$(date +%s%N)

    # TODO Measure until bench is finished

    ssh_ready_ms=$(( (ssh_ready_ns - start_ns) / 1000000 ))
	pg_ready_ms=$(( (pg_ready_ns - start_ns) / 1000000 ))
	mig_done_ms=$(( (mig_done_ns - start_ns) / 1000000 ))
    ssh2_ready_ms=$(( (ssh2_ready_ns - start_ns) / 1000000 ))
	pg2_ready_ms=$(( (pg2_ready_ns - start_ns) / 1000000 ))

    echo "$run,$ssh_ready_ms,$pg_ready_ms,$mig_done_ms,$ssh2_ready_ms,$pg2_ready_ms" >> "$OUT_CSV"
	echo "run $run: ssh=${ssh_ready_ms} ms, postgres=${pg_ready_ms} ms, mig_finished=${mig_done_ms},ssh_dest=${ssh2_ready_ms}, postgres_dest=${pg2_ready_ms}"

	qmp_cmd "$src_sock" system_powerdown

	qmp_cmd "$dst_sock" system_powerdown

    cleanup
    trap - EXIT
done
echo
echo "Results written to $OUT_CSV"
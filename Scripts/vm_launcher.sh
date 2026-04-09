#!/bin/bash
set -euo pipefail

SRC="${1:?Source image path}"
DST="${2:?Destination image path}"

SRC_QMP="/tmp/src-qmp.sock"
DST_QMP="/tmp/dst-qmp.sock"
MIG_SOCK="/tmp/mig.sock"

qemu-system-x86_64 \
	-accel kvm \
	-m 4G \
	-smp 4 \
	-cpu host \
	-drive file="$DST",if=virtio,format=qcow2 \
	-qmp unix:"$DST_QMP",server=on,wait=off \
	-incoming unix:"$MIG_SOCK" \
	-display none \
	-daemonize

qemu-system-x86_64 \
	-accel kvm \
  	-m 4G \
  	-smp 4 \
  	-cpu host \
  	-drive file="$SRC",if=virtio,format=qcow2 \
  	-qmp unix:"$SRC_QMP",server=on,wait=off \
  	-display none \
  	-daemonize

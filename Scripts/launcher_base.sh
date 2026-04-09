#!/bin/bash

SRC="${1:?Source image path}"
DST="${2:?Destination image path}"

qemu-system-x86_64 source_arch_qemu -accel kvm -drive file= -m 4G -smp 4 -cpu host &
qemu-system-x86_64 dest_arch_qemu -accel kvm -boot order=d -m 4G -smp 4 -cpu host &

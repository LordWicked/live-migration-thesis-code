#!/bin/bash

printf '%s\n' \
  '{"execute":"qmp_capabilities"}' \
  '{"execute":"system_powerdown"}' \
| socat - UNIX-CONNECT:/tmp/dst-qmp.sock

printf '%s\n' \
  '{"execute":"qmp_capabilities"}' \
  '{"execute":"system_powerdown"}' \
| socat - UNIX-CONNECT:/tmp/src-qmp.sock

#!/bin/bash

printf '%s\n' \
  '{"execute":"qmp_capabilities"}' \
  '{"execute":"query-status"}' \
| socat - UNIX-CONNECT:/tmp/dst-qmp.sock

printf '%s\n' \
  '{"execute":"qmp_capabilities"}' \
  '{"execute":"query-status"}' \
| socat - UNIX-CONNECT:/tmp/src-qmp.sock

#!/bin/bash

printf '%s\n' \
  '{"execute":"qmp_capabilities"}' \
  '{"execute":"query-migrate"}' \
| socat - UNIX-CONNECT:/tmp/src-qmp.sock

#!/bin/bash

printf '%s\n' \
  '{"execute":"qmp_capabilities"}' \
  '{"execute":"migrate","arguments":{"uri":"unix:/tmp/mig.sock"}}' \
| socat - UNIX-CONNECT:/tmp/src-qmp.sock

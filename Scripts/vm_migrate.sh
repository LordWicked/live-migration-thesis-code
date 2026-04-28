#!/bin/bash

SOURCE_SOCK="{1:Source sock}"

printf '%s\n' \
  '{"execute":"qmp_capabilities"}' \
  '{"execute":"migrate","arguments":{"uri":"unix:/tmp/mig.sock"}}' \
| socat - UNIX-CONNECT:"$SOURCE_SOCK"

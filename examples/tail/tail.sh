#!/usr/bin/env bash
# Stream a growing file to one reader: its last lines, then every line
# appended, until you stop it. Options after the file go to fofoca-stream.
#
#   examples/tail/tail.sh app.log --web-url http://127.0.0.1:3020/
#
# FOFOCA_STREAM names the binary when it is not on PATH.
set -euo pipefail

file=${1:?usage: tail.sh <file> [fofoca-stream options]}
shift

# Not a pipeline: `tail -f` never ends by itself, and a pipeline waits for it
# after fofoca-stream has exited. Through a FIFO, the trap stops it instead.
fifo=$(mktemp -u "${TMPDIR:-/tmp}/tail-stream.XXXXXX")
mkfifo "$fifo"
tail_pid=
trap 'rm -f "$fifo"; [ -n "$tail_pid" ] && kill "$tail_pid" 2>/dev/null; true' EXIT
tail -f "$file" > "$fifo" &
tail_pid=$!
"${FOFOCA_STREAM:-fofoca-stream}" "$@" < "$fifo"

#!/bin/sh
SRV=$(pgrep -f 'server.py' | head -1)
CONN=$(( $(netstat -an -p tcp | grep -c ESTABLISHED) / 2 ))
FDS=$(procstat -f "$SRV" 2>/dev/null | tail -n +2 | wc -l | tr -d ' ')
RSS=$(ps -p "$SRV" -o rss= | tr -d ' ')
PCPU=$(ps -p "$SRV" -o pcpu= | tr -d ' ')
DENIED=$(netstat -m | awk '/requests for mbufs denied/{print $1}')
printf '%s | conns=%s | srv_mem=%sKB | srv_cpu=%s%% | srv_fds=%s | openfiles=%s/%s | mbuf_denied=%s\n' \
  "$(date +%T)" "$CONN" "$RSS" "$PCPU" "$FDS" "$(sysctl -n kern.openfiles)" "$(sysctl -n kern.maxfiles)" "$DENIED"

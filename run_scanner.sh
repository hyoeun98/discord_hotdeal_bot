#!/bin/bash

LOGFILE="script.log"

while true; do
    echo "$(date '+%Y-%m-%d %H:%M:%S'): scanner start" >> $LOGFILE
    python scanner.py
    echo "$(date '+%Y-%m-%d %H:%M:%S'): scanner error" >> $LOGFILE
    sleep 5  # 5초 대기
done

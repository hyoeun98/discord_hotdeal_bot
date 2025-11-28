#!/bin/bash

cd /home/hyoeun/hotdeal_bot
UV_PATH="/home/hyoeun/.local/bin/uv"
export AWS_SHARED_CREDENTIALS_FILE=/home/hyoeun/.aws/credentials
export AWS_CONFIG_FILE=/home/hyoeun/.aws/config
export AWS_DEFAULT_REGION=ap-northeast-2
export PLAYWRIGHT_BROWSERS_PATH=/home/hyoeun/.cache/ms-playwright
# 로그 파일 위치
LOG_DIR="/home/hyoeun/hotdeal_bot/scanner/logs"
mkdir -p "$LOG_DIR"

# 현재 시각 출력
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting scanner batch..." >> "$LOG_DIR/scanner.log"

# 실행할 사이트 리스트
SITES=(
    "PPOM_PPU"
    "FM_KOREA"
    "QUASAR_ZONE"
    "RULI_WEB"
    "EOMI_SAE"
    "ARCA_LIVE"
    "COOL_ENJOY"
)

# PID 배열
PIDS=()



# 각 사이트를 백그라운드로 실행
for SITE in "${SITES[@]}"; do
    echo "[$(date '+%H:%M:%S')] Starting $SITE..." >> "$LOG_DIR/scanner.log"
    $UV_PATH run python /home/hyoeun/hotdeal_bot/scanner/scanner.py "$SITE" >> "$LOG_DIR/${SITE}.log" 2>&1 &
    PIDS+=($!)
done

# 모든 프로세스가 완료될 때까지 대기
for i in "${!PIDS[@]}"; do
    PID=${PIDS[$i]}
    SITE=${SITES[$i]}
    
    wait $PID
    EXIT_CODE=$?
    
    if [ $EXIT_CODE -eq 0 ]; then
        echo "[$(date '+%H:%M:%S')] ✓ Done $SITE (Success)" >> "$LOG_DIR/scanner.log"
    else
        echo "[$(date '+%H:%M:%S')] ✗ Done $SITE (Failed with code $EXIT_CODE)" >> "$LOG_DIR/scanner.log"
    fi
done

echo "[$(date '+%Y-%m-%d %H:%M:%S')] All scanners finished." >> "$LOG_DIR/scanner.log"
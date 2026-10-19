#!/usr/bin/env bash
# Xem hoặc đổi ngưỡng cảnh báo (Grafana áp dụng ở lần đánh giá kế tiếp, ~10 giây).
#   scripts/set-threshold.sh                          # xem ngưỡng hiện tại
#   scripts/set-threshold.sh withdraw_count 3         # cảnh báo khi rút > 3 lần
#   scripts/set-threshold.sh withdraw_window_min 2    # trong 2 phút
#   scripts/set-threshold.sh large_amount 500000000   # giao dịch > 500 triệu VND
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] && set -a && . ./.env && set +a

ch() {
  docker exec -i rtdp-clickhouse clickhouse-client \
    --user "${CLICKHOUSE_USER:-rtdp}" --password "${CLICKHOUSE_PASSWORD:-rtdp}" "$@"
}

if [ $# -eq 2 ]; then
  case "$1" in
    withdraw_count|withdraw_window_min|large_amount) ;;
    *) echo "Tên ngưỡng không hợp lệ: $1 (withdraw_count | withdraw_window_min | large_amount)"; exit 1 ;;
  esac
  [[ "$2" =~ ^[0-9]+([.][0-9]+)?$ ]] || { echo "Giá trị phải là số: $2"; exit 1; }
  ch -q "INSERT INTO rtdp.alert_config (name, value) VALUES ('$1', $2)"
elif [ $# -ne 0 ]; then
  sed -n '2,6p' "$0"; exit 1
fi

ch -q "SELECT name, value, updated_at FROM rtdp.alert_config FINAL ORDER BY name FORMAT PrettyCompact"

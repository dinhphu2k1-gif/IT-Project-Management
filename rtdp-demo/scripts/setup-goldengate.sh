#!/usr/bin/env bash
# Cấu hình GoldenGate thật (container ogg-oracle, ogg-daa) cho demo RTDP. Chạy được nhiều lần.
#   1. ogg-oracle: credential, supplemental log schema CORE, Integrated Extract EXTCORE -> trail ct
#   2. ogg-daa:    đặt file cấu hình Kafka Handler, Replicat RKAFKA đọc trail ct -> Kafka
#   3. Bật Auto Start / Auto Restart cho cả hai
# Yêu cầu: docker compose -f docker-compose.yml -f docker-compose.goldengate.yml up -d
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] && set -a && . ./.env && set +a
U="${OGG_ADMIN_USER:-oggadmin}"; P="${OGG_ADMIN_PASSWORD:-Oggadmin_123}"; GGP="${GGADMIN_PASSWORD:-ggadmin}"
ORA_URL=http://localhost:9012; DAA_URL=http://localhost:9022
ORA_DEP=/u01/app/oracle/deployments/ORA_DEPLOY; DAA_DEP=/u01/app/oracle/deployments/DAA_DEPLOY
OGG=/u01/app/oracle/product/ogg

wait_http() { # url
  for _ in $(seq 1 60); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 -u "$U:$P" "$1/services/v2/config/summary")" = 200 ] && return 0
    sleep 3
  done
  echo "Không kết nối được $1 (container đã chạy chưa?)"; exit 1
}
exists() { [ "$(curl -s -o /dev/null -w '%{http_code}' -u "$U:$P" "$1")" = 200 ]; }
status() { curl -s -u "$U:$P" "$1" | python3 -c "import sys,json;print(json.load(sys.stdin).get('response',{}).get('status','none'))"; }
wait_running() { # url name
  for _ in $(seq 1 30); do [ "$(status "$1")" = running ] && { echo "  $2: running"; return 0; }; sleep 2; done
  echo "  $2 chưa chạy, xem: docker exec rtdp-ogg-oracle tail $ORA_DEP/var/log/ggserr.log (hoặc rtdp-ogg-daa, $DAA_DEP)"; exit 1
}
profile() { # base-url resource-url
  curl -s -u "$U:$P" -H Content-Type:application/json -X POST \
    "$1/services/v2/config/types/ogg:managedProcessSettings/values/rtdp-auto" \
    -d '{"$schema":"ogg:managedProcessSettings","autoStart":{"enabled":true,"delay":0},"autoRestart":{"enabled":true,"onSuccess":false,"delay":0,"retries":10,"window":60,"disableOnFailure":false}}' >/dev/null || true
  curl -s -u "$U:$P" -H Content-Type:application/json -X PATCH "$2" -d '{"managedProcessSettings":"rtdp-auto"}' >/dev/null
}

wait_http "$ORA_URL"; wait_http "$DAA_URL"

echo "== 1. Extract (ogg-oracle)"
if exists "$ORA_URL/services/v2/extracts/EXTCORE"; then
  echo "  EXTCORE đã tồn tại, bỏ qua tạo mới"
else
  docker exec rtdp-ogg-oracle cp /home/oracle/rtdp-gg/extract/EXTCORE.prm "$ORA_DEP/etc/conf/ogg/EXTCORE.prm"
  # Sau khi xoá volume GoldenGate, Oracle vẫn nhớ Extract cũ ("already registered"): gỡ riêng các đối tượng của EXTCORE
  docker exec -i rtdp-oracle sqlplus -s / as sysdba <<'SQL' >/dev/null
WHENEVER SQLERROR CONTINUE
ALTER SESSION SET CONTAINER = FREEPDB1;
BEGIN
  FOR c IN (SELECT capture_name FROM dba_capture WHERE capture_name = 'OGG$CAP_EXTCORE') LOOP
    DBMS_CAPTURE_ADM.DROP_CAPTURE(c.capture_name, drop_unused_rule_sets => TRUE);
  END LOOP;
  FOR a IN (SELECT apply_name FROM dba_apply WHERE apply_name = 'OGG$EXTCORE') LOOP
    DBMS_APPLY_ADM.DROP_APPLY(a.apply_name, drop_unused_rule_sets => TRUE);
  END LOOP;
  FOR q IN (SELECT owner FROM dba_queues WHERE name = 'OGG$Q_EXTCORE') LOOP
    DBMS_STREAMS_ADM.REMOVE_QUEUE(q.owner || '.OGG$Q_EXTCORE', cascade => TRUE, drop_unused_queue_table => TRUE);
  END LOOP;
END;
/
SQL
  # Thứ tự quan trọng: ADD EXTRACT trước, REGISTER sau (REGISTER trước thì Extract abend vì thiếu logmining server)
  docker exec -i rtdp-ogg-oracle adminclient <<EOF | grep -E "OGG-[0-9]+|ERROR" || true
CONNECT http://ogg-oracle:9011 DEPLOYMENT ORA_DEPLOY AS $U PASSWORD $P
ADD CREDENTIALSTORE
ALTER CREDENTIALSTORE ADD USER ggadmin@oracle:1521/FREEPDB1 PASSWORD $GGP ALIAS ggadmin_pdb DOMAIN OracleGoldenGate
DBLOGIN USERIDALIAS ggadmin_pdb DOMAIN OracleGoldenGate
ADD SCHEMATRANDATA CORE ALLCOLS
ADD EXTRACT EXTCORE, INTEGRATED TRANLOG, BEGIN NOW
ADD EXTTRAIL ct, EXTRACT EXTCORE
REGISTER EXTRACT EXTCORE DATABASE
START EXTRACT EXTCORE
EOF
fi
wait_running "$ORA_URL/services/v2/extracts/EXTCORE" EXTCORE

echo "== 2. Replicat Kafka (ogg-daa)"
docker exec rtdp-ogg-daa bash -c "
  set -e
  D=$DAA_DEP/etc/conf/ogg; G=/home/oracle/rtdp-gg/daa
  sed -e 's|<OGG_ETC_HOME>|$DAA_DEP/etc|g' \$G/RKAFKA.prm > \$D/RKAFKA.prm
  sed -e 's|<OGG_HOME>|$OGG|g' -e 's|<OGG_ETC_HOME>|$DAA_DEP/etc|g' \$G/kafka.props > \$D/kafka.props
  sed -e 's|^bootstrap.servers=.*|bootstrap.servers=kafka:29092|' \$G/custom_kafka_producer.properties > \$D/custom_kafka_producer.properties"
if exists "$DAA_URL/services/v2/replicats/RKAFKA"; then
  echo "  RKAFKA đã tồn tại, khởi động lại để nhận cấu hình mới"
  docker exec -i rtdp-ogg-daa adminclient <<EOF | grep -E "OGG-[0-9]+|ERROR" || true
CONNECT http://ogg-daa:9021 DEPLOYMENT DAA_DEPLOY AS $U PASSWORD $P
STOP REPLICAT RKAFKA
START REPLICAT RKAFKA
EOF
else
  docker exec -i rtdp-ogg-daa adminclient <<EOF | grep -E "OGG-[0-9]+|ERROR" || true
CONNECT http://ogg-daa:9021 DEPLOYMENT DAA_DEPLOY AS $U PASSWORD $P
ADD REPLICAT RKAFKA, EXTTRAIL /u01/app/oracle/trail/ct
START REPLICAT RKAFKA
EOF
fi
wait_running "$DAA_URL/services/v2/replicats/RKAFKA" RKAFKA

echo "== 3. Auto Start / Auto Restart"
profile "$ORA_URL" "$ORA_URL/services/v2/extracts/EXTCORE"
profile "$DAA_URL" "$DAA_URL/services/v2/replicats/RKAFKA"
echo "  đã gán profile rtdp-auto cho EXTCORE và RKAFKA"

cat <<MSG

Xong. Nhớ tắt bộ giả lập nếu đang chạy:  pkill -f scripts/gg_simulator.py
Giao diện web:  Oracle  http://localhost:9011   DAA  http://localhost:9021   (tài khoản $U)
MSG

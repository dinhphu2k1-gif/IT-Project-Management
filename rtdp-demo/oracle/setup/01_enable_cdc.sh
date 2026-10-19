#!/bin/bash
# Chạy MỘT LẦN khi container Oracle khởi tạo database lần đầu.
# Bật các điều kiện cần cho GoldenGate Integrated Extract:
#   ARCHIVELOG, FORCE LOGGING, supplemental log mức database,
#   ENABLE_GOLDENGATE_REPLICATION, streams pool cho LogMiner.

mkdir -p /opt/oracle/oradata/archive

sqlplus -s / as sysdba <<'SQL'
SET ECHO ON FEEDBACK ON
WHENEVER SQLERROR CONTINUE

ALTER SYSTEM SET log_archive_dest_1='LOCATION=/opt/oracle/oradata/archive' SCOPE=SPFILE;
ALTER SYSTEM SET enable_goldengate_replication=TRUE SCOPE=SPFILE;
ALTER SYSTEM SET streams_pool_size=256M SCOPE=SPFILE;

SHUTDOWN IMMEDIATE
STARTUP MOUNT
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;

ALTER DATABASE FORCE LOGGING;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER PLUGGABLE DATABASE ALL OPEN;
ALTER PLUGGABLE DATABASE FREEPDB1 SAVE STATE;
ALTER SYSTEM SWITCH LOGFILE;

SELECT log_mode, force_logging, supplemental_log_data_min FROM v$database;
SHOW PARAMETER enable_goldengate_replication
SQL

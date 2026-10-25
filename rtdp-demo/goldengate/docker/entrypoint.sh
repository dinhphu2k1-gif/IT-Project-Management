#!/bin/bash
# Khởi tạo Service Manager + deployment ở lần chạy đầu, rồi chạy Service Manager.
# Biến môi trường (thêm TNS_ADMIN_DIR, OGG_SCHEMA cho bản Oracle): DEPLOY_NAME, PORT_BASE (cổng Service Manager; Admin/Distribution/Receiver/Metrics = +1..+4),
#                  OGG_DATA_HOME (tuỳ chọn, thư mục trail), OGG_ADMIN_USER, OGG_ADMIN_PASSWORD, ENV_LD_LIBRARY_PATH (tuỳ chọn)
set -e
DEPLOY_NAME=${DEPLOY_NAME:?}
PORT_BASE=${PORT_BASE:?}
SM_HOME=/u01/app/oracle/ggsm
DEP_HOME=/u01/app/oracle/deployments/${DEPLOY_NAME}

if [ ! -f "${DEP_HOME}/etc/conf/deploymentConfiguration.dat" ]; then
  echo "== Lần đầu: tạo Service Manager và deployment ${DEPLOY_NAME}"
  cat > /tmp/oggca.rsp <<RSP
oracle.install.responseFileVersion=/oracle/install/rspfmt_oggca_response_schema_v23_1_0
CONFIGURATION_OPTION=ADD
DEPLOYMENT_NAME=${DEPLOY_NAME}
ADMINISTRATOR_USER=${OGG_ADMIN_USER}
ADMINISTRATOR_PASSWORD=${OGG_ADMIN_PASSWORD}
SERVICEMANAGER_DEPLOYMENT_HOME=${SM_HOME}
HOST_SERVICEMANAGER=${HOSTNAME}
PORT_SERVICEMANAGER=${PORT_BASE}
SECURITY_ENABLED=false
STRONG_PWD_POLICY_ENABLED=false
CREATE_NEW_SERVICEMANAGER=true
REGISTER_SERVICEMANAGER_AS_A_SERVICE=false
INTEGRATE_SERVICEMANAGER_WITH_XAG=false
EXISTING_SERVICEMANAGER_IS_XAG_ENABLED=false
OGG_SOFTWARE_HOME=${OGG_SOFTWARE_HOME}
OGG_DEPLOYMENT_HOME=${DEP_HOME}
OGG_ETC_HOME=${DEP_HOME}/etc
OGG_CONF_HOME=${DEP_HOME}/etc/conf
OGG_SSL_HOME=${DEP_HOME}/etc/ssl
OGG_VAR_HOME=${DEP_HOME}/var
OGG_DATA_HOME=${OGG_DATA_HOME:-${DEP_HOME}/var/lib/data}
ENV_LD_LIBRARY_PATH=${ENV_LD_LIBRARY_PATH:-${OGG_SOFTWARE_HOME}/lib}
ENV_USER_VARS=
__TNS__
__SCHEMA__SHARDING_ENABLED=false
ADMINISTRATION_SERVER_ENABLED=true
PORT_ADMINSRVR=$((PORT_BASE + 1))
DISTRIBUTION_SERVER_ENABLED=true
PORT_DISTSRVR=$((PORT_BASE + 2))
NON_SECURE_DISTSRVR_CONNECTS_TO_SECURE_RCVRSRVR=false
RECEIVER_SERVER_ENABLED=true
PORT_RCVRSRVR=$((PORT_BASE + 3))
METRICS_SERVER_ENABLED=true
PORT_PMSRVR=$((PORT_BASE + 4))
UDP_PORT_PMSRVR=$((PORT_BASE + 4))
PMSRVR_DATASTORE_TYPE=LMDB
RSP
  if [ -n "${TNS_ADMIN_DIR:-}" ]; then
    mkdir -p "${TNS_ADMIN_DIR}"
    sed -i "s|^__TNS__|ENV_TNS_ADMIN=${TNS_ADMIN_DIR}|" /tmp/oggca.rsp
  else
    sed -i '/^__TNS__/d' /tmp/oggca.rsp
  fi
  if [ -n "${OGG_SCHEMA:-}" ]; then
    sed -i "s|^__SCHEMA__|OGG_SCHEMA=${OGG_SCHEMA}|" /tmp/oggca.rsp
  else
    sed -i '/^__SCHEMA__/d' /tmp/oggca.rsp
  fi
  # oggca.sh tự khởi động Service Manager sau khi tạo xong
  "${OGG_SOFTWARE_HOME}/bin/oggca.sh" -silent -responseFile /tmp/oggca.rsp
  rm -f /tmp/oggca.rsp
else
  export OGG_VAR_HOME=${SM_HOME}/var OGG_ETC_HOME=${SM_HOME}/etc
  "${OGG_SOFTWARE_HOME}/bin/ServiceManager" >/dev/null 2>&1 &
fi

# Giữ container sống theo nhật ký của Service Manager
for _ in $(seq 1 60); do [ -f "${SM_HOME}/var/log/ServiceManager.log" ] && break; sleep 1; done
exec tail -F "${SM_HOME}/var/log/ServiceManager.log"

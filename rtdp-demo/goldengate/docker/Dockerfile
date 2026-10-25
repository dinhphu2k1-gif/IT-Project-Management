# syntax=docker/dockerfile:1
# Hai image GoldenGate dựng từ bộ cài Oracle tải về (không nằm trong repo):
#   target "ora": GoldenGate for Oracle (Microservices), chạy Integrated Extract
#   target "daa": GoldenGate for Big Data / DAA, chạy Replicat Kafka
# Thư mục chứa file zip được truyền qua build context tên "installer" (xem docker-compose.goldengate.yml).

FROM oraclelinux:8 AS base
RUN dnf -y install unzip tar vi libnsl libaio openssl which findutils procps-ng && dnf clean all
RUN groupadd -g 1000 oinstall && \
    useradd -u 1000 -g oinstall -m -d /home/oracle -s /bin/bash oracle && \
    mkdir -p /u01/app/oracle/product/ogg /u01/app/oracle/deployments /u01/app/oracle/ggsm /u01/app/oraInventory && \
    chown -R oracle:oinstall /u01
USER oracle
WORKDIR /home/oracle
RUN printf 'inventory_loc=/u01/app/oraInventory\ninst_group=oinstall\n' > /home/oracle/oraInst.loc
ENV OGG_SOFTWARE_HOME=/u01/app/oracle/product/ogg
ENV PATH=/u01/app/oracle/product/ogg/bin:$PATH

# ---------------------------------------------------------------------------
FROM base AS ora
ARG OGG_ORA_ZIP=V1054774-01.zip
RUN --mount=type=bind,from=installer,source=${OGG_ORA_ZIP},target=/tmp/ogg.zip \
    mkdir /tmp/inst && cd /tmp/inst && unzip -q /tmp/ogg.zip && \
    printf '%s\n' \
      'oracle.install.responseFileVersion=/oracle/install/rspfmt_ogginstall_response_schema_v23_1_0' \
      'INSTALL_OPTION=ora23ai' \
      'SOFTWARE_LOCATION=/u01/app/oracle/product/ogg' \
      'INVENTORY_LOCATION=/u01/app/oraInventory' \
      'UNIX_GROUP_NAME=oinstall' > /tmp/oggcore.rsp && \
    ./fbo_ggs_Linux_x64_Oracle_services_shiphome/Disk1/runInstaller -silent -waitForCompletion \
      -ignoreSysPrereqs -responseFile /tmp/oggcore.rsp -invPtrLoc /home/oracle/oraInst.loc; \
    rc=$?; rm -rf /tmp/inst; test -x /u01/app/oracle/product/ogg/bin/ServiceManager || { echo "cài đặt lỗi (rc=$rc)"; exit 1; }

# Thư mục trail dùng chung giữa hai container (mount volume ogg-trail vào đây)
RUN mkdir -p /u01/app/oracle/trail
COPY --chown=oracle:oinstall --chmod=755 entrypoint.sh /home/oracle/entrypoint.sh
ENTRYPOINT ["/home/oracle/entrypoint.sh"]

# ---------------------------------------------------------------------------
FROM base AS daa
USER root
RUN dnf -y install java-11-openjdk-headless curl && dnf clean all
USER oracle
ARG OGG_DAA_ZIP=V1054834-01.zip
RUN --mount=type=bind,from=installer,source=${OGG_DAA_ZIP},target=/tmp/ogg.zip \
    mkdir /tmp/inst && cd /tmp/inst && unzip -q /tmp/ogg.zip && \
    printf '%s\n' \
      'oracle.install.responseFileVersion=/oracle/install/rspfmt_ogginstall_response_schema_v21_1_0' \
      'INSTALL_OPTION=Generic' \
      'SOFTWARE_LOCATION=/u01/app/oracle/product/ogg' \
      'INVENTORY_LOCATION=/u01/app/oraInventory' \
      'UNIX_GROUP_NAME=oinstall' > /tmp/oggcore.rsp && \
    ./ggs_Linux_x64_BigData_services_shiphome/Disk1/runInstaller -silent -waitForCompletion \
      -ignoreSysPrereqs -responseFile /tmp/oggcore.rsp -invPtrLoc /home/oracle/oraInst.loc; \
    rc=$?; rm -rf /tmp/inst; test -x /u01/app/oracle/product/ogg/bin/ServiceManager || { echo "cài đặt lỗi (rc=$rc)"; exit 1; }
# Thư viện Kafka client cho Kafka Handler (README mục 3.2)
RUN cd /u01/app/oracle/product/ogg/opt/DependencyDownloader && ./kafka.sh 3.8.1

# Thư mục trail dùng chung giữa hai container (mount volume ogg-trail vào đây)
RUN mkdir -p /u01/app/oracle/trail
COPY --chown=oracle:oinstall --chmod=755 entrypoint.sh /home/oracle/entrypoint.sh
ENTRYPOINT ["/home/oracle/entrypoint.sh"]

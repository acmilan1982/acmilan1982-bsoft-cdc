#!/bin/bash

DIR="$(cd "$(dirname "$0")"; pwd)"
APP_JAR="$DIR/sync-server-1.0-SNAPSHOT.jar"
LIB_DIR="$DIR/lib"
CONF_DIR="$DIR/config"
LOG_DIR="$DIR/logs"

[ -d "$LOG_DIR" ] || mkdir -p "$LOG_DIR"

# 选 java：优先 JAVA_HOME，否则 PATH
if [ -n "$JAVA_HOME" ] && [ -x "$JAVA_HOME/bin/java" ]; then
  JAVA_EXE="$JAVA_HOME/bin/java"
else
  JAVA_EXE="java"
fi

JAVA_OPTS="${JAVA_OPTS} -Xms512m -Xmx1024m -Dfile.encoding=UTF-8 -Duser.timezone=Asia/Shanghai"

# =========================
# [MOD] 1) 不再使用 "$LIB_DIR/*"（JVM 扫描 jar 顺序不稳定，会导致重复类随机命中）
#        改为：手工拼接一个“确定顺序”的 classpath：
#        - 先放 flink-connector-debezium / flink-connector-oracle-cdc（确保它们的类优先加载）
#        - 再把其余 jar 按文件名排序追加（保证每次启动顺序一致）
# =========================
FIRST_JARS=$(ls -1 \
  "$LIB_DIR"/flink-connector-debezium-*.jar \
  "$LIB_DIR"/flink-connector-oracle-cdc-*.jar 2>/dev/null)

OTHER_JARS=$(find "$LIB_DIR" -maxdepth 1 -name "*.jar" \
  ! -name "flink-connector-debezium-*.jar" \
  ! -name "flink-connector-oracle-cdc-*.jar" \
  -printf "%p\n" | sort)

# [MOD] 2) 组装 CP（用冒号拼接）
CP="$APP_JAR"
if [ -n "$FIRST_JARS" ]; then
  CP="$CP:$(echo "$FIRST_JARS" | paste -sd: -)"
fi
if [ -n "$OTHER_JARS" ]; then
  CP="$CP:$(echo "$OTHER_JARS" | paste -sd: -)"
fi
# =========================

echo "Using: $JAVA_EXE"
$JAVA_EXE -version

echo "=== Launching Sync Server (PROD环境，后台运行，日志见 $LOG_DIR/sync-server.log) ==="
nohup $JAVA_EXE $JAVA_OPTS -cp "$CP" com.bsoft.server.bootapp.ServerApplication \
  --spring.profiles.active=prod \
  --spring.config.additional-location="$CONF_DIR/" \
  >> "$LOG_DIR/sync-server.log" 2>&1 &

echo "sync-server 已启动，日志见 $LOG_DIR/sync-server.log"

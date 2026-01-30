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
CP="$APP_JAR:$LIB_DIR/*"

echo "Using: $JAVA_EXE"
$JAVA_EXE -version

echo "=== Launching Sync Server (DEV环境，后台运行，日志见 $LOG_DIR/sync-server.log) ==="
nohup $JAVA_EXE $JAVA_OPTS -cp "$CP" com.bsoft.server.bootapp.ServerApplication \
  --spring.profiles.active=dev \
  --spring.config.additional-location="$CONF_DIR/" \
  >> "$LOG_DIR/sync-server.log" 2>&1 &

echo "sync-server 已启动，日志见 $LOG_DIR/sync-server.log"

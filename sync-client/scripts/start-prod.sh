#!/bin/bash

DIR="$(cd "$(dirname "$0")"; pwd)"
APP_JAR="$DIR/sync-client-1.0-SNAPSHOT.jar"
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


# =====================================================
# ✅ 方案1：JVM 堆内存优化（核心改动）
# =====================================================
JAVA_OPTS="${JAVA_OPTS} -Xms16g -Xmx16g"                # 保持总堆 16GB
JAVA_OPTS="${JAVA_OPTS} -Dfile.encoding=UTF-8"
JAVA_OPTS="${JAVA_OPTS} -Duser.timezone=Asia/Shanghai"

# ✅ 使用 G1 GC（相比 Parallel GC 更适合大堆场景）
JAVA_OPTS="${JAVA_OPTS} -XX:+UseG1GC"

# ✅ 优化 Young 区比例（默认 NewRatio=3 改为 1）

JAVA_OPTS="${JAVA_OPTS} -XX:NewRatio:=1"

# ✅ Survivor 区比例（Eden:Survivor = 8:1）
JAVA_OPTS="${JAVA_OPTS} -XX:SurvivorRatio=8"

# ✅ G1 特定优化参数
JAVA_OPTS="${JAVA_OPTS} -XX:MaxGCPauseMillis=200"          # 最大停顿时间 200ms
JAVA_OPTS="${JAVA_OPTS} -XX:G1HeapRegionSize=16m"         # 堆区域大小 16MB
JAVA_OPTS="${JAVA_OPTS} -XX:InitiatingHeapOccupancyPercent=45"  # Old Gen 达到 45% 触发并发标记
JAVA_OPTS="${JAVA_OPTS} -XX:+ParallelRefProcEnabled"      # 并行处理 Reference 对象
JAVA_OPTS="${JAVA_OPTS} -XX:ConcGCThreads=8"              # 并发 GC 线程数（CPU 核心数 1/4）


# ✅ OOM 时自动生成堆转储
JAVA_OPTS="${JAVA_OPTS} -XX:+HeapDumpOnOutOfMemoryError"
JAVA_OPTS="${JAVA_OPTS} -XX:HeapDumpPath=$LOG_DIR/heapdump.hprof"

# ✅ 优化大对象分配（避免频繁晋升到 Old Gen）
JAVA_OPTS="${JAVA_OPTS} -XX:PretenureSizeThreshold=1m"    # 大于 1MB 的对象直接进入 Old Gen
# =====================================================


# =========================
# [MOD] 1) 不再使用 "$LIB_DIR/*" 让 JVM 自己扫描（顺序不稳定，会导致重复类随机命中）
#        改为：手工拼接一个“确定顺序”的 classpath：
#        - 先放 flink-connector-debezium / flink-connector-oracle-cdc（确保 public 版 EmbeddedEngineChangeEvent 先被加载）
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

echo "=== Launching Sync Client (PROD环境，后台运行，日志见 $LOG_DIR/sync-client.log) ==="
nohup $JAVA_EXE $JAVA_OPTS -cp "$CP" com.bsoft.client.bootapp.ClientApplication \
  --spring.profiles.active=prod \
  --spring.config.additional-location="$CONF_DIR/" \
  > /dev/null 2>&1 &

echo "sync-client 已启动，日志见 $LOG_DIR/sync-client.log"

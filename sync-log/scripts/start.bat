@echo off
setlocal ENABLEDELAYEDEXPANSION
chcp 65001 > nul

set "DIR=%~dp0"
set "APP_JAR=%DIR%sync-log-1.0-SNAPSHOT.jar"
set "LIB_DIR=%DIR%lib"
set "CONF_DIR=%DIR%config"
set "LOG_DIR=%DIR%logs"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

rem 选择 Java：优先 JAVA_HOME，否则 PATH
if defined JAVA_HOME (
    if exist "%JAVA_HOME%\bin\java.exe" (
        set "JAVA_EXE=%JAVA_HOME%\bin\java.exe"
    ) else (
        set "JAVA_EXE=java"
    )
) else (
    set "JAVA_EXE=java"
)

rem =====================================================
rem JVM 堆内存优化（与 Linux 版本保持一致）
rem =====================================================
set "JAVA_OPTS=%JAVA_OPTS% -Xms8g -Xmx8g"
set "JAVA_OPTS=%JAVA_OPTS% -Dfile.encoding=UTF-8"
set "JAVA_OPTS=%JAVA_OPTS% -Duser.timezone=Asia/Shanghai"

rem 使用 G1 GC
set "JAVA_OPTS=%JAVA_OPTS% -XX:+UseG1GC"

rem 优化 Young 区比例
set "JAVA_OPTS=%JAVA_OPTS% -XX:NewRatio=1"
set "JAVA_OPTS=%JAVA_OPTS% -XX:SurvivorRatio=8"

rem G1 特定优化参数
set "JAVA_OPTS=%JAVA_OPTS% -XX:MaxGCPauseMillis=200"
set "JAVA_OPTS=%JAVA_OPTS% -XX:G1HeapRegionSize=16m"
set "JAVA_OPTS=%JAVA_OPTS% -XX:InitiatingHeapOccupancyPercent=45"
set "JAVA_OPTS=%JAVA_OPTS% -XX:+ParallelRefProcEnabled"
set "JAVA_OPTS=%JAVA_OPTS% -XX:ConcGCThreads=8"

rem OOM 时自动生成堆转储
set "JAVA_OPTS=%JAVA_OPTS% -XX:+HeapDumpOnOutOfMemoryError"
set "JAVA_OPTS=%JAVA_OPTS% -XX:HeapDumpPath=%LOG_DIR%\heapdump.hprof"

rem 优化大对象分配
set "JAVA_OPTS=%JAVA_OPTS% -XX:PretenureSizeThreshold=1m"
rem =====================================================

rem 组装 Classpath（优先加载 Flink CDC 相关 JAR）
set "CP=%APP_JAR%"
for %%f in ("%LIB_DIR%\flink-connector-debezium-*.jar") do set "CP=!CP!;%%f"
for %%f in ("%LIB_DIR%\flink-connector-oracle-cdc-*.jar") do set "CP=!CP!;%%f"
for %%f in ("%LIB_DIR%\*.jar") do (
    echo %%~nxf | findstr /i "flink-connector-debezium flink-connector-oracle-cdc" > nul
    if errorlevel 1 set "CP=!CP!;%%f"
)

echo Using: %JAVA_EXE%
"%JAVA_EXE%" -version

echo === Launching Sync Log Server (PROD环境，后台运行) ===
echo sync-log 已启动，日志见 %LOG_DIR%\sync-log.log

start /b "" "%JAVA_EXE%" %JAVA_OPTS% -cp "%CP%" com.bsoft.log.bootapp.LogApplication ^
  --spring.profiles.active=prod ^
  --spring.config.additional-location=%CONF_DIR%\ ^
  > nul 2>&1

echo sync-log 已启动，日志见 %LOG_DIR%\sync-log.log

@echo off
setlocal ENABLEDELAYEDEXPANSION
chcp 65001 > nul

set "DIR=%~dp0"
set "APP_JAR=%DIR%sync-client-1.0-SNAPSHOT.jar"
set "LIB_DIR=%DIR%lib"
set "CONF_DIR=%DIR%config"
set "LOG_DIR=%DIR%logs"
set "CP_FILE=%TEMP%\sync-client-cp.txt"

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
set "JAVA_OPTS=%JAVA_OPTS% -Xms16g -Xmx16g"
set "JAVA_OPTS=%JAVA_OPTS% -Dfile.encoding=UTF-8"
set "JAVA_OPTS=%JAVA_OPTS% -Duser.timezone=Asia/Shanghai"
set "JAVA_OPTS=%JAVA_OPTS% -XX:+UseG1GC"
set "JAVA_OPTS=%JAVA_OPTS% -XX:NewRatio=1"
set "JAVA_OPTS=%JAVA_OPTS% -XX:SurvivorRatio=8"
set "JAVA_OPTS=%JAVA_OPTS% -XX:MaxGCPauseMillis=200"
set "JAVA_OPTS=%JAVA_OPTS% -XX:G1HeapRegionSize=16m"
set "JAVA_OPTS=%JAVA_OPTS% -XX:InitiatingHeapOccupancyPercent=45"
set "JAVA_OPTS=%JAVA_OPTS% -XX:+ParallelRefProcEnabled"
set "JAVA_OPTS=%JAVA_OPTS% -XX:ConcGCThreads=8"
set "JAVA_OPTS=%JAVA_OPTS% -XX:+HeapDumpOnOutOfMemoryError"
set "JAVA_OPTS=%JAVA_OPTS% -XX:HeapDumpPath=%LOG_DIR%\heapdump.hprof"
set "JAVA_OPTS=%JAVA_OPTS% -XX:PretenureSizeThreshold=1m"
rem =====================================================

rem 组装 Classpath（写入文件避免命令行长度限制）
echo %APP_JAR% > "%CP_FILE%"
for %%f in ("%LIB_DIR%\flink-connector-debezium-*.jar") do echo %%f >> "%CP_FILE%"
for %%f in ("%LIB_DIR%\flink-connector-oracle-cdc-*.jar") do echo %%f >> "%CP_FILE%"
for %%f in ("%LIB_DIR%\*.jar") do (
    echo %%~nxf | findstr /i "flink-connector-debezium flink-connector-oracle-cdc" > nul
    if errorlevel 1 echo %%f >> "%CP_FILE%"
)

rem 将文件内容转为分号分隔的字符串
set "CP="
for /f "usebackq delims=" %%i in ("%CP_FILE%") do (
    if defined CP (set "CP=!CP!;%%i") else (set "CP=%%i")
)

echo Using: %JAVA_EXE%
"%JAVA_EXE%" -version

echo === Launching Sync Client (PROD环境，后台运行，日志见 %LOG_DIR%\sync-client.log) ===

start /b "" "%JAVA_EXE%" %JAVA_OPTS% -cp "%CP%" com.bsoft.client.bootapp.ClientApplication ^
  --spring.profiles.active=prod ^
  --spring.config.additional-location=%CONF_DIR%\ ^
  > nul 2>&1

del "%CP_FILE%" 2>nul
echo sync-client 已启动，日志见 %LOG_DIR%\sync-client.log

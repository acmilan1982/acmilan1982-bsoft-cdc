@echo off
setlocal ENABLEDELAYEDEXPANSION
chcp 65001 > nul

set "DIR=%~dp0"
set "APP_JAR=%DIR%sync-client-1.0-SNAPSHOT.jar"
set "LIB_DIR=%DIR%lib"
set "CONF_DIR=%DIR%config"
set "LOG_DIR=%DIR%logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

rem 选 java：优先 JAVA_HOME，否则用 PATH 上的 java
set "JAVA_EXE=%JAVA_HOME%\bin\java"
if not exist "%JAVA_EXE%" set "JAVA_EXE=java"

set "JAVA_OPTS=%JAVA_OPTS% -Xms512m -Xmx1024m -Dfile.encoding=UTF-8 -Duser.timezone=Asia/Shanghai"
set "CP=%APP_JAR%;%LIB_DIR%\*"

echo Using: %JAVA_EXE%
"%JAVA_EXE%" -version

echo === Launching Sync Client (console + daily rolling file via Spring Boot) ===
"%JAVA_EXE%" %JAVA_OPTS% -cp "%CP%" com.bsoft.client.bootapp.ClientApplication ^
  --spring.profiles.active=dev ^
  --spring.config.additional-location=%CONF_DIR%/

echo === ExitCode: %ERRORLEVEL% ===
pause

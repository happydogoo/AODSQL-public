@echo off
REM demo2.bat - 仅演示 demo2.sql（Windows版）
REM 用法：demo2.bat

setlocal
cd /d %~dp0\..
set PYTHONPATH=%cd%

ECHO 🚀 AODSQL demo2.sql 单文件演示
ECHO ==================================

REM 计时开始
set STARTTIME=%TIME%

ECHO ===============================
ECHO 01. 运行 demo2.sql
ECHO ===============================
python -m cli.main < sql\demo\demo2.sql

REM 计时结束
set ENDTIME=%TIME%

REM 计算耗时（秒）
set /A STARTSEC=(1%STARTTIME:~0,2%-100)*3600 + (1%STARTTIME:~3,2%-100)*60 + (1%STARTTIME:~6,2%-100)
set /A ENDSEC=(1%ENDTIME:~0,2%-100)*3600 + (1%ENDTIME:~3,2%-100)*60 + (1%ENDTIME:~6,2%-100)
set /A DURSEC=ENDSEC-STARTSEC

ECHO ===============================
ECHO ✅ demo2.sql 演示完成！
ECHO 用时：%DURSEC% 秒
ECHO ===============================
endlocal 
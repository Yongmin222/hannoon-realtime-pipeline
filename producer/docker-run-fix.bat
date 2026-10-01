@echo off
chcp 65001 > nul
echo Kafka Producer Docker 실행 (포트 충돌 해결)

:MENU
cls
echo ===============================================
echo   포트 충돌 해결 후 Docker 실행
echo ===============================================
echo.
echo 1. 포트 없이 실행 (추천 - Kafka Producer만)
echo 2. 다른 포트(8081)로 실행
echo 3. 8080 포트 사용 프로세스 확인
echo 4. Java 프로세스 모두 종료 후 실행
echo 5. 컨테이너 상태 확인
echo 6. 로그 확인
echo 0. 종료
echo.
set /p choice=선택하세요 (0-6): 

if "%choice%"=="1" goto RUN_NOPORT
if "%choice%"=="2" goto RUN_DIFFERENT_PORT
if "%choice%"=="3" goto CHECK_PORT
if "%choice%"=="4" goto KILL_AND_RUN
if "%choice%"=="5" goto CHECK_STATUS
if "%choice%"=="6" goto CHECK_LOGS
if "%choice%"=="0" goto EXIT
goto MENU

:RUN_NOPORT
echo.
echo 기존 컨테이너 정리...
docker compose down 2>nul
echo.
echo 포트 없이 실행 중...
docker compose -f docker-compose.noport.yml up -d --build
if errorlevel 1 (
    echo 실행 실패!
    pause
    goto MENU
)
echo.
echo 성공! 컨테이너가 백그라운드에서 실행 중입니다.
echo 로그 확인: docker logs -f kafka-purchase-producer
pause
goto MENU

:RUN_DIFFERENT_PORT
echo.
echo 기존 컨테이너 정리...
docker compose down 2>nul
echo.
echo 포트 8081로 실행 중...
docker compose up -d --build
if errorlevel 1 (
    echo 실행 실패!
    pause
    goto MENU
)
echo.
echo 성공! 컨테이너가 8081 포트로 실행 중입니다.
echo 웹 접속: http://localhost:8081/actuator/health
pause
goto MENU

:CHECK_PORT
echo.
echo 8080 포트 사용 중인 프로세스:
netstat -ano | findstr :8080
echo.
pause
goto MENU

:KILL_AND_RUN
echo.
echo Java 프로세스 종료 중...
taskkill /IM java.exe /F 2>nul
echo 기존 컨테이너 정리...
docker compose down 2>nul
echo.
echo 포트 없이 실행 중...
docker compose -f docker-compose.noport.yml up -d --build
if errorlevel 1 (
    echo 실행 실패!
    pause
    goto MENU
)
echo.
echo 성공!
pause
goto MENU

:CHECK_STATUS
echo.
echo 컨테이너 상태:
docker ps | findstr kafka-purchase-producer
echo.
pause
goto MENU

:CHECK_LOGS
echo.
echo 실시간 로그 출력 중... (Ctrl+C로 종료)
docker logs -f kafka-purchase-producer
pause
goto MENU

:EXIT
echo 프로그램을 종료합니다.
exit /b 0

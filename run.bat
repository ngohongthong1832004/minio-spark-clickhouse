@echo off
SETLOCAL

echo [0/4] Building custom images (spark)...
docker-compose build || exit /b

echo [1/4] Initializing Airflow DB and creating admin user...
docker-compose run --rm airflow-init || exit /b

echo [2/4] Starting up the full system...
docker-compose up -d || exit /b

echo [3/4] Checking container status...
docker-compose ps

echo.
echo [4/4] The system is ready!
echo Access Airflow at: http://localhost:8080
echo Username: admin
echo Password: admin

ENDLOCAL
@REM pause

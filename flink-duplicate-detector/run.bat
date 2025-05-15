@echo off
echo Building Duplicate Payment Detector Application...
gradlew clean shadowJar
echo.
echo Build complete. JAR file: build/libs/duplicate-payment-detector.jar
echo.
echo Starting application...
java -jar build/libs/duplicate-payment-detector.jar

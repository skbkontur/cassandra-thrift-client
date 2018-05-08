FOR /F "skip=2 tokens=2*" %%A IN ('REG QUERY "HKLM\Software\JavaSoft\Java Development Kit\1.8" /v JavaHome') DO set JAVA_HOME=%%B
"%JAVA_HOME%\bin\javac.exe" -target 1.8 SystemTimerTester.java || exit /b 1
"%JAVA_HOME%\bin\java.exe" -classpath .\ SystemTimerTester || exit /b 1
pause
SystemTimerTester is a tool to determine whether you need to use patched Cassandra or not.

Cassandra uses method LockSupport.parkNanos which behavior is platform-dependent. 
On Windows due to low timer resolution this method will suspend thread for at least 1 ms even if passed argument would be less. 
On Linux this method works fine. 
But situation becomes more tricky when you use Linux vitual machine on Windows host. 
In such enviroment LockSupport.parkNanos may work wrong too. 
So, this script just calls LockSupport.parkNanos many times and compares expected sleep time with actual. 
If they differ too much, script will warn you.

Usage:
```
javac -target 1.8 SystemTimerTester.java
java SystemTimerTester
```
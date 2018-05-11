using System;
using System.Diagnostics;
using System.IO;
using NUnit.Framework;
using SkbKontur.Cassandra.Local;

namespace CassandraLocal.Tests
{
    public class SystemTimer_Tests
    {
        [TestCase(13)]
        [TestCase(31)]
        public void CheckSystemTimerResolution(int testDurationSeconds)
        {
            var java8Home = JavaHomeHelpers.TryGetJavaHome(JavaHomeHelpers.Jdk8Key);
            if (string.IsNullOrWhiteSpace(java8Home))
                throw new InvalidOperationException("JDK 8 64-bit home directory is not found");

            var systemTimerTesterDirectory = DirectoryHelpers.FindDirectory(AppDomain.CurrentDomain.BaseDirectory, "SystemTimerTester");
            Console.Out.WriteLine($"systemTimerTesterDirectory: {systemTimerTesterDirectory}");

            Run(command: Path.Combine(java8Home, "bin/javac.exe"), args: "-target 1.8 SystemTimerTester.java", workingDirectory: systemTimerTesterDirectory);

            Run(command: Path.Combine(java8Home, "bin/java.exe"), args: $"-classpath ./ SystemTimerTester {testDurationSeconds}", workingDirectory: systemTimerTesterDirectory);
        }

        private static void Run(string command, string args, string workingDirectory)
        {
            var process = new Process
                {
                    StartInfo =
                        {
                            FileName = command,
                            WorkingDirectory = workingDirectory,
                            Arguments = args,
                            UseShellExecute = false,
                            CreateNoWindow = false,
                            WindowStyle = ProcessWindowStyle.Normal,
                            RedirectStandardError = true,
                            RedirectStandardOutput = true,
                        }
                };
            process.Start();
            Console.Out.WriteLine("{0} stdout > {1}", command, process.StandardOutput.ReadToEnd());
            Console.Out.WriteLine("{0} stderr > {1}", command, process.StandardError.ReadToEnd());
            process.WaitForExit((int)TimeSpan.FromMinutes(1).TotalMilliseconds);
            Assert.That(process.ExitCode, Is.EqualTo(0));
        }
    }
}
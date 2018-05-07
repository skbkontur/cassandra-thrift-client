using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Management;
using System.Threading;
using Microsoft.Win32;

namespace SkbKontur.Cassandra.Local
{
    public static class LocalCassandraProcessManager
    {
        private const string localCassandraNodeNameMarker = "skbkontur.local.cassandra.node.name";

        public static string StartLocalCassandraProcess(string cassandraDirectory)
        {
            var cassandraShellProcess = new Process
                {
                    StartInfo =
                        {
                            FileName = Path.Combine(cassandraDirectory, @"bin/cassandra.bat"),
                            WorkingDirectory = Path.Combine(cassandraDirectory, @"bin"),
                            Arguments = "LEGACY",
                            UseShellExecute = false,
                            CreateNoWindow = false,
                            WindowStyle = ProcessWindowStyle.Normal,
                        }
                };
            cassandraShellProcess.StartInfo.EnvironmentVariables["JAVA_HOME"] = GetJava8Home();
            cassandraShellProcess.Start();
            WaitForCassandraToStart(cassandraDirectory);
            var localNodeName = GetLocalCassandraNodeName(cassandraShellProcess);
            return localNodeName;
        }

        private static string GetJava8Home()
        {
            const string jdkKey = @"Software\JavaSoft\Java Development Kit\1.8";
            const string jreKey = @"Software\JavaSoft\Java Runtime Environment\1.8";
            var java8Home = TryGetJavaHome(jreKey) ?? TryGetJavaHome(jdkKey);
            if (string.IsNullOrWhiteSpace(java8Home))
                throw new InvalidOperationException("Java 8 64-bit home directory is not found");
            return java8Home;
        }

        private static string TryGetJavaHome(string javaKey)
        {
            using (var hklm64 = RegistryKey.OpenBaseKey(RegistryHive.LocalMachine, RegistryView.Registry64))
            using (var registryKey = hklm64.OpenSubKey(javaKey))
                return (string)registryKey?.GetValue("JavaHome");
        }

        private static void WaitForCassandraToStart(string cassandraDirectory)
        {
            var sw = Stopwatch.StartNew();
            var logFileName = Path.Combine(cassandraDirectory, @"logs/system.log");
            while (sw.Elapsed < TimeSpan.FromSeconds(30))
            {
                if (File.Exists(logFileName))
                {
                    using (var file = new FileStream(logFileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                    using (var reader = new StreamReader(file))
                    {
                        while (true)
                        {
                            var logContent = reader.ReadLine();
                            if (!string.IsNullOrEmpty(logContent) && logContent.Contains("Listening for thrift clients..."))
                                return;
                        }
                    }
                }
                Thread.Sleep(TimeSpan.FromMilliseconds(300));
            }
            throw new InvalidOperationException($"Failed to start cassandra in: {cassandraDirectory}");
        }

        public static void StopAllLocalCassandraProcesses()
        {
            foreach (var cassandraPid in GetAllLocalCassandraProcessIds())
                Process.GetProcessById(cassandraPid).Kill();
            while (GetAllLocalCassandraProcessIds().Any())
                Thread.Sleep(TimeSpan.FromMilliseconds(300));
        }

        public static void StopLocalCassandraProcess(string localNodeName)
        {
            foreach (var cassandraPid in GetLocalCassandraProcessIds(localNodeName))
                Process.GetProcessById(cassandraPid).Kill();
            while (GetLocalCassandraProcessIds(localNodeName).Any())
                Thread.Sleep(TimeSpan.FromMilliseconds(300));
        }

        public static List<int> GetAllLocalCassandraProcessIds()
        {
            return DoGetLocalCassandraProcessIds(localNodeNameOrNothing: null);
        }

        public static List<int> GetLocalCassandraProcessIds(string localNodeName)
        {
            if (string.IsNullOrEmpty(localNodeName))
                throw new InvalidOperationException($"{nameof(localNodeName)} is empty");
            return DoGetLocalCassandraProcessIds(localNodeName);
        }

        private static List<int> DoGetLocalCassandraProcessIds(string localNodeNameOrNothing)
        {
            var cassandraPids = new List<int>();
            using (var searcher = new ManagementObjectSearcher("SELECT ProcessId, CommandLine FROM Win32_Process"))
            {
                foreach (var o in searcher.Get())
                {
                    var mo = (ManagementObject)o;
                    if (mo["CommandLine"] == null)
                        continue;
                    var commandLine = mo["CommandLine"].ToString();
                    var cassandraPid = int.Parse(mo["ProcessId"].ToString());
                    if (string.IsNullOrEmpty(localNodeNameOrNothing))
                    {
                        if (commandLine.Contains(localCassandraNodeNameMarker))
                            cassandraPids.Add(cassandraPid);
                    }
                    else
                    {
                        if (commandLine.Contains($"{localCassandraNodeNameMarker}={localNodeNameOrNothing}"))
                            cassandraPids.Add(cassandraPid);
                    }
                }
            }
            return cassandraPids;
        }

        private static string GetLocalCassandraNodeName(Process cassandraShellProcess)
        {
            using (var searcher = new ManagementObjectSearcher($"SELECT CommandLine FROM Win32_Process WHERE ParentProcessId = {cassandraShellProcess.Id}"))
            {
                var commandLine = searcher.Get().Cast<ManagementObject>().Single()["CommandLine"].ToString();
                var patternToMatch= $"{localCassandraNodeNameMarker}=";
                var startPos = commandLine.IndexOf(patternToMatch, StringComparison.InvariantCultureIgnoreCase) + patternToMatch.Length;
                var endPos = commandLine.IndexOf(' ', startPos);
                return commandLine.Substring(startPos, endPos - startPos);
            }
        }
    }
}
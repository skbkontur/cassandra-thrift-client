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
    public class LocalCassandraNode
    {
        public LocalCassandraNode(string templateDirectory, string deployDirectory)
        {
            TemplateDirectory = templateDirectory;
            DeployDirectory = deployDirectory;
            ClusterName = "local_cluster";
            LocalNodeName = "local_node";
            HeapSize = "1G";
            const string localhostAddress = "127.0.0.1";
            RpcAddress = localhostAddress;
            ListenAddress = localhostAddress;
            SeedAddresses = new[] {localhostAddress};
            RpcPort = 9360;
            CqlPort = 9343;
            JmxPort = 7399;
            GossipPort = 7400;
        }

        public string TemplateDirectory { get; }
        public string DeployDirectory { get; }
        public string ClusterName { get; set; }
        public string LocalNodeName { get; set; }
        public string HeapSize { get; set; }
        public string RpcAddress { get; set; }
        public string ListenAddress { get; set; }
        public string[] SeedAddresses { get; set; }
        public int RpcPort { get; set; }
        public int CqlPort { get; set; }
        public int JmxPort { get; set; }
        public int GossipPort { get; set; }

        public override string ToString()
        {
            return $"{nameof(TemplateDirectory)}: {TemplateDirectory}, " +
                   $"{nameof(DeployDirectory)}: {DeployDirectory}, " +
                   $"{nameof(ClusterName)}: {ClusterName}, " +
                   $"{nameof(LocalNodeName)}: {LocalNodeName}, " +
                   $"{nameof(HeapSize)}: {HeapSize}, " +
                   $"{nameof(RpcAddress)}: {RpcAddress}, " +
                   $"{nameof(ListenAddress)}: {ListenAddress}, " +
                   $"{nameof(SeedAddresses)}: {string.Join(";", SeedAddresses)}, " +
                   $"{nameof(RpcPort)}: {RpcPort}, " +
                   $"{nameof(CqlPort)}: {CqlPort}, " +
                   $"{nameof(JmxPort)}: {JmxPort}, " +
                   $"{nameof(GossipPort)}: {GossipPort}";
        }

        public void Restart()
        {
            Stop();
            Deploy();
            Start();
            WaitForStart();
        }

        public void Stop()
        {
            foreach (var processId in GetLocalCassandraProcessIds(LocalNodeName))
                Process.GetProcessById(processId).Kill();

            while (GetLocalCassandraProcessIds(LocalNodeName).Any())
                Thread.Sleep(TimeSpan.FromMilliseconds(300));
        }

        private void WaitForStart()
        {
            var sw = Stopwatch.StartNew();
            var logFileName = Path.Combine(DeployDirectory, @"logs\system.log");
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
                Thread.Sleep(TimeSpan.FromMilliseconds(500));
            }
            throw new InvalidOperationException($"Failed to start cassandra node: {this}");
        }

        private void Start()
        {
            var process = new Process
                {
                    StartInfo =
                        {
                            FileName = Path.Combine(DeployDirectory, @"bin\cassandra.bat"),
                            WorkingDirectory = Path.Combine(DeployDirectory, @"bin"),
                            Arguments = "LEGACY",
                            UseShellExecute = false,
                            CreateNoWindow = false,
                            WindowStyle = ProcessWindowStyle.Normal,
                        }
                };
            process.StartInfo.EnvironmentVariables["JAVA_HOME"] = GetJava8Home();
            process.Start();
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

        public void Deploy()
        {
            if (Directory.Exists(DeployDirectory))
                Directory.Delete(DeployDirectory, recursive: true);
            Directory.CreateDirectory(DeployDirectory);

            DirectoryCopy(TemplateDirectory, DeployDirectory, copySubDirectories: true);

            ExpandSettingsTemplates(DeployDirectory);
        }

        private void ExpandSettingsTemplates(string deployDirectory)
        {
            var templateFiles = new[]
                {
                    @"conf\cassandra.yaml",
                    @"bin\cassandra.bat"
                };
            foreach (var templateFile in templateFiles)
                ExpandSettingsTemplate(Path.Combine(deployDirectory, templateFile));
        }

        private void ExpandSettingsTemplate(string templateFilePath)
        {
            ExpandSettingsTemplate(templateFilePath, new Dictionary<string, string>
                {
                    {"LocalNodeName", LocalNodeName},
                    {"JmxPort", JmxPort.ToString()},
                    {"GossipPort", GossipPort.ToString()},
                    {"RpcPort", RpcPort.ToString()},
                    {"CqlPort", CqlPort.ToString()},
                    {"ListenAddress", ListenAddress},
                    {"RpcAddress", RpcAddress},
                    {"SeedAddresses", string.Join(",", SeedAddresses)},
                    {"ClusterName", ClusterName},
                    {"HeapSize", HeapSize}
                });
        }

        private static void ExpandSettingsTemplate(string templateFilePath, Dictionary<string, string> values)
        {
            var template = File.ReadAllText(templateFilePath);
            var settings = values.Aggregate(template, (current, value) => current.Replace("{{" + value.Key + "}}", value.Value));
            File.WriteAllText(templateFilePath, settings);
        }

        private static void DirectoryCopy(string sourceDirectoryName, string destinationDirectoryName, bool copySubDirectories)
        {
            var sourceDirectoryInfo = new DirectoryInfo(sourceDirectoryName);

            if (!sourceDirectoryInfo.Exists)
                throw new InvalidOperationException($"Source directory does not exist or could not be found: {sourceDirectoryName}");

            if (!Directory.Exists(destinationDirectoryName))
                Directory.CreateDirectory(destinationDirectoryName);

            var files = sourceDirectoryInfo.GetFiles();
            foreach (var file in files)
                file.CopyTo(Path.Combine(destinationDirectoryName, file.Name), overwrite: false);

            if (copySubDirectories)
            {
                var subDirectories = sourceDirectoryInfo.GetDirectories();
                foreach (var subDirectory in subDirectories)
                    DirectoryCopy(subDirectory.FullName, Path.Combine(destinationDirectoryName, subDirectory.Name), copySubDirectories: true);
            }
        }

        public static IEnumerable<int> GetLocalCassandraProcessIds(string localNodeName)
        {
            using (var searcher = new ManagementObjectSearcher("SELECT ProcessId, CommandLine FROM Win32_Process"))
            {
                foreach (var o in searcher.Get())
                {
                    var mo = (ManagementObject)o;
                    if (mo["CommandLine"] != null && mo["CommandLine"].ToString().Contains($"skbkontur.local.node.name={localNodeName}"))
                        yield return int.Parse(mo["ProcessId"].ToString());
                }
            }
        }
    }
}
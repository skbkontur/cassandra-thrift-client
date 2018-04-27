using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Management;
using System.Threading;

using Vostok.Logging;
using Vostok.Logging.Extensions;

namespace SKBKontur.Cassandra.ClusterDeployment
{
    public class CassandraNode
    {
        public CassandraNode(string templateDirectory, string deployDirectory, ILog logger)
        {
            this.templateDirectory = templateDirectory;
            this.logger = logger;
            Name = "node_at_9360";
            JmxPort = 7399;
            GossipPort = 7400;
            RpcPort = 9360;
            CqlPort = 9343;
            DeployDirectory = deployDirectory;
            ListenAddress = "127.0.0.1";
            RpcAddress = "127.0.0.1";
            SeedAddresses = new[] {"127.0.0.1"};
            ClusterName = "test_cluster";
            HeapSize = "1G";
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
            foreach(var processId in GetCassandraProcessIds(Name))
            {
                Process.GetProcessById(processId).Kill();
                logger.Info("Kill cassandra process id={0}", processId);
            }

            Console.WriteLine("Start waiting for cassandra process stop.");
            while(GetCassandraProcessIds(Name).Any())
            {
                logger.Info("Waiting for cassandra process stop.");
                Thread.Sleep(1000);
            }
        }

        public string Name { get; set; }

        public int JmxPort { get; set; }
        public int GossipPort { get; set; }
        public int RpcPort { get; set; }
        public string DeployDirectory { get; set; }
        public string ListenAddress { get; set; }
        public string RpcAddress { get; set; }
        public string[] SeedAddresses { get; set; }
        public string ClusterName { get; set; }
        public int CqlPort { get; set; }
        public string HeapSize { get; set; }

        public override string ToString()
        {
            return string.Format("TemplateDirectory: {0}, Name: {1}, JmxPort: {2}, GossipPort: {3}, RpcPort: {4}, DeployDirectory: {5}, ListenAddress: {6}, RpcAddress: {7}, ClusterName: {8}, CqlPort: {9}, HeapSize: {10}",
                                 templateDirectory, Name, JmxPort, GossipPort, RpcPort, DeployDirectory, ListenAddress, RpcAddress, ClusterName, CqlPort, HeapSize);
        }

        private void WaitForStart()
        {
            var sw = Stopwatch.StartNew();
            var logFileName = Path.Combine(DeployDirectory, @"logs\system.log");
            while(sw.Elapsed < TimeSpan.FromSeconds(30))
            {
                if(File.Exists(logFileName))
                {
                    using(var file = new FileStream(logFileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                    using(var reader = new StreamReader(file))
                    {
                        while(true)
                        {
                            var logContent = reader.ReadLine();
                            if(!string.IsNullOrEmpty(logContent) && logContent.Contains("Listening for thrift clients..."))
                                return;
                        }
                    }
                }
                Thread.Sleep(500);
            }
            throw new InvalidOperationException(string.Format("Failed to start cassandra node: {0}", this));
        }

        private void Start()
        {
            var proc = new Process();
            proc.StartInfo.FileName = Path.Combine(DeployDirectory, @"bin\cassandra.bat");
            proc.StartInfo.WorkingDirectory = Path.Combine(DeployDirectory, @"bin");
            proc.StartInfo.Arguments = "LEGACY";
            proc.StartInfo.WindowStyle = ProcessWindowStyle.Normal;
            proc.StartInfo.CreateNoWindow = true;
            proc.StartInfo.UseShellExecute = true;
            proc.Start();
        }

        public void Deploy()
        {
            if(Directory.Exists(DeployDirectory))
            {
                Directory.Delete(DeployDirectory, true);
                Console.WriteLine("Deleted existed directory {0}.", DeployDirectory);
            }

            Directory.CreateDirectory(DeployDirectory);
            Console.WriteLine("Deploying cassandra to {0}.", DeployDirectory);
            DirectoryCopy(templateDirectory, DeployDirectory, true);
            Console.WriteLine("Cassandra deployed to {0}.", DeployDirectory);

            PatchSettings(DeployDirectory);
        }

        private void PatchSettings(string deployDirectory)
        {
            var filesToPatch = new[]
                {
                    @"conf\cassandra.yaml",
                    @"bin\cassandra.bat"
                };
            foreach(var file in filesToPatch)
                PathSettingsInFile(Path.Combine(deployDirectory, file));
        }

        private void PathSettingsInFile(string filePath)
        {
            PathSettingsInFile(filePath, new Dictionary<string, string>
                {
                    {"NodeInternalName", Name},
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

        private void PathSettingsInFile(string filePath, Dictionary<string, string> values)
        {
            File.WriteAllText(
                filePath,
                values.Aggregate(
                    File.ReadAllText(filePath),
                    (current, value) => current.Replace("{{" + value.Key + "}}", value.Value))
                );
        }

        private static void DirectoryCopy(string sourceDirectoryName, string destinationDirectoryName, bool copySubDirectories)
        {
            var sourceDirectoryInfo = new DirectoryInfo(sourceDirectoryName);

            if(!sourceDirectoryInfo.Exists)
                throw new DirectoryNotFoundException(string.Format("Source directory does not exist or could not be found: {0}", sourceDirectoryName));

            if(!Directory.Exists(destinationDirectoryName))
                Directory.CreateDirectory(destinationDirectoryName);

            var files = sourceDirectoryInfo.GetFiles();
            foreach(var file in files)
                file.CopyTo(Path.Combine(destinationDirectoryName, file.Name), false);

            if(copySubDirectories)
            {
                var subDirectories = sourceDirectoryInfo.GetDirectories();
                foreach(var subDirectory in subDirectories)
                    DirectoryCopy(subDirectory.FullName, Path.Combine(destinationDirectoryName, subDirectory.Name), true);
            }
        }

        private IEnumerable<int> GetCassandraProcessIds(string name)
        {
            using(var mos = new ManagementObjectSearcher("SELECT ProcessId, CommandLine FROM Win32_Process"))
            {
                foreach(ManagementObject mo in mos.Get())
                {
                    if(mo["CommandLine"] != null && mo["CommandLine"].ToString().Contains(string.Format("internalflag={0}", name)))
                        yield return int.Parse(mo["ProcessId"].ToString());
                }
            }
        }

        private readonly string templateDirectory;
        private readonly ILog logger;
    }
}
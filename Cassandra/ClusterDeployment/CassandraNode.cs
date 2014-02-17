using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Management;
using System.Threading;

using log4net;

namespace SKBKontur.Cassandra.FunctionalTests.Management
{
    public class CassandraNode
    {
        public CassandraNode(string templateDirectory)
        {
            this.templateDirectory = templateDirectory;
        }

        public void Restart()
        {
            Stop();
            Deploy();
            Start();
            WaitForStart();
        }

        private void WaitForStart()
        {
            var logFileName = Path.Combine(DeployDirectory, @"bin", DataBaseDirectory, @"log\system.log");
            while(true)
            {
                if(File.Exists(logFileName))
                {
                    using(var file = new FileStream(logFileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                    {
                        using(var reader = new StreamReader(file))
                        {
                            while(true)
                            {
                                var logContent = reader.ReadLine();
                                if (!string.IsNullOrEmpty(logContent) && logContent.Contains("Listening for thrift clients..."))
                                    return;                                
                            }
                        }
                    }
                }
                Thread.Sleep(500);
            }
            
        }

        public void Stop()
        {
            foreach(var processId in GetCassandraProcessIds(Name))
            {
                Process.GetProcessById(processId).Kill();
                logger.InfoFormat("Kill cassandra process id={0}", processId);
            }

            Console.WriteLine("Start waiting for cassandra process stop.");
            while(GetCassandraProcessIds(Name).Any())
            {
                logger.InfoFormat("Waiting for cassandra process stop.");
                Thread.Sleep(1000);
            }
        }

        public string Name { get; set; }

        public int JmxPort { get; set; }
        public int GossipPort { get; set; }
        public int RpcPort { get; set; }
        public string DataBaseDirectory { get; set; }
        public string DeployDirectory { get; set; }
        public string ListenAddress { get; set; }
        public string RpsAddress { get; set; }
        public string[] SeedAddresses { get; set; }
        public string InitialToken { get; set; }
        public string ClusterName { get; set; }
        public int CqlPort { get; set; }

        private void Start()
        {
            var proc = new Process();
            proc.StartInfo.FileName = Path.Combine(DeployDirectory, @"bin\cassandra.bat");
            proc.StartInfo.WorkingDirectory = Path.Combine(DeployDirectory, @"bin");
            proc.StartInfo.WindowStyle = ProcessWindowStyle.Normal;
            proc.StartInfo.CreateNoWindow = true;
            proc.StartInfo.UseShellExecute = true;
            proc.Start();
        }

        private void Deploy()
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
                    @"conf\log4j-server.properties",
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
                    {"DataBaseDirectory", DataBaseDirectory},
                    {"DeployDirectory", DeployDirectory},
                    {"ListenAddress", ListenAddress},
                    {"RpsAddress", RpsAddress},
                    {"SeedAddresses", string.Join(",", SeedAddresses)},
                    {"InitialToken", InitialToken},
                    {"ClusterName", ClusterName}
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

        private readonly ILog logger = LogManager.GetLogger(typeof(CassandraNode));
    }
}
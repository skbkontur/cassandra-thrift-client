using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using NUnit.Framework;
using SkbKontur.Cassandra.Local;

namespace CassandraLocal.Tests
{
    public class LocalCassandraNodeManager_Tests
    {
        [TestCase("2.2.x")]
        [TestCase("3.11.x")]
        public void Restart(string cassandraTemplateVersion)
        {
            var templateDirectory = DirectoryHelpers.FindDirectory(AppDomain.CurrentDomain.BaseDirectory, $@"cassandra-local/cassandra/v{cassandraTemplateVersion}");
            Console.Out.WriteLine($"templateDirectory: {templateDirectory}");

            var deployDirectory = Path.Combine(Path.GetTempPath(), $"deployed_cassandra_v{cassandraTemplateVersion}");
            Console.Out.WriteLine($"deployDirectory: {deployDirectory}");

            var beforeStartTimestamp = DateTime.Now;
            var node = new LocalCassandraNode(templateDirectory, deployDirectory)
                {
                    LocalNodeName = Guid.NewGuid().ToString("N")
                };

            node.Restart();

            Assert.That(Directory.Exists(deployDirectory));
            var nodePid = LocalCassandraProcessManager.GetAllLocalCassandraProcessIds().Single();
            var nodeProcess = Process.GetProcessById(nodePid);
            Console.Out.WriteLine($"cassandra node process: [{nodePid}] {nodeProcess.ProcessName}");
            Assert.That(nodeProcess.StartTime > beforeStartTimestamp);

            node.Stop();

            Assert.That(nodeProcess.HasExited);
        }
    }
}
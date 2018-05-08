using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using NUnit.Framework;
using SkbKontur.Cassandra.Local;

namespace CassandraLocal.Tests
{
    public class LocalCassandraNode_Tests
    {
        [Test]
        public void Restart()
        {
            var templateDirectory = DirectoryHelpers.FindDirectory(AppDomain.CurrentDomain.BaseDirectory, @"cassandra-local/cassandra/v2.2.x");
            Console.Out.WriteLine($"templateDirectory: {templateDirectory}");

            var deployDirectory = Path.Combine(Path.GetTempPath(), "deployed_cassandra");
            Console.Out.WriteLine($"deployDirectory: {deployDirectory}");

            var beforeStartTimestamp = DateTime.Now;
            var localNodeName = Guid.NewGuid().ToString("N");
            var node = new LocalCassandraNode(templateDirectory, deployDirectory)
                {
                    LocalNodeName = localNodeName
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
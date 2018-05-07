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
        private const string cassandraTemplates = @"cassandra-local/cassandra/v2.2.x";

        [Test]
        public void Restart()
        {
            var templateDirectory = FindCassandraTemplateDirectory(AppDomain.CurrentDomain.BaseDirectory);
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

        private static string FindCassandraTemplateDirectory(string currentDir)
        {
            var cassandraTemplateDirectory = Path.Combine(currentDir, cassandraTemplates);
            return Directory.Exists(cassandraTemplateDirectory) ? cassandraTemplateDirectory : FindCassandraTemplateDirectory(Path.GetDirectoryName(currentDir));
        }
    }
}
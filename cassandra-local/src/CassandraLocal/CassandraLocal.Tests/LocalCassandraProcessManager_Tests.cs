using System;
using System.IO;
using NUnit.Framework;
using SkbKontur.Cassandra.Local;

namespace CassandraLocal.Tests
{
    public class LocalCassandraProcessManager_Tests
    {
        [Test]
        public void StopAllLocalCassandraProcesses()
        {
            Assert.That(LocalCassandraProcessManager.GetAllLocalCassandraProcessIds(), Is.Empty, "no cassandra process should be running before test");

            StartLocalCassandra(cassandraTemplateVersion: "2.2.x", instanceId: 1);
            Assert.That(LocalCassandraProcessManager.GetAllLocalCassandraProcessIds().Count, Is.EqualTo(1));

            StartLocalCassandra(cassandraTemplateVersion: "3.11.x", instanceId: 2);
            Assert.That(LocalCassandraProcessManager.GetAllLocalCassandraProcessIds().Count, Is.EqualTo(2));

            LocalCassandraProcessManager.StopAllLocalCassandraProcesses();
            Assert.That(LocalCassandraProcessManager.GetAllLocalCassandraProcessIds(), Is.Empty, "no cassandra process should be running after test");
        }

        private static void StartLocalCassandra(string cassandraTemplateVersion, int instanceId)
        {
            var templateDirectory = DirectoryHelpers.FindDirectory(AppDomain.CurrentDomain.BaseDirectory, $@"cassandra-local/cassandra/v{cassandraTemplateVersion}");
            Console.Out.WriteLine($"templateDirectory: {templateDirectory}");

            var deployDirectory = Path.Combine(Path.GetTempPath(), $"deployed_cassandra_v{cassandraTemplateVersion}");
            Console.Out.WriteLine($"deployDirectory: {deployDirectory}");

            var localNodeName = Guid.NewGuid().ToString("N");
            var node = new LocalCassandraNode(templateDirectory, deployDirectory)
                {
                    LocalNodeName = localNodeName,
                    ClusterName = cassandraTemplateVersion,
                    RpcPort = 9160 + instanceId,
                    CqlPort = 9042 + instanceId,
                    JmxPort = 7199 + instanceId,
                    GossipPort = 7000 + instanceId,
                };
            node.Deploy();

            var actualLocalNodeName = LocalCassandraProcessManager.StartLocalCassandraProcess(node.DeployDirectory);
            Assert.That(actualLocalNodeName, Is.EqualTo(localNodeName));

            LocalCassandraProcessManager.WaitForLocalCassandraPortsToOpen(node.RpcPort, node.CqlPort);
        }
    }
}
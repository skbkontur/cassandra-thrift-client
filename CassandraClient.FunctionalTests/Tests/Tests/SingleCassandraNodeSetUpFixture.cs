using System;
using System.IO;

using NUnit.Framework;

using SKBKontur.Cassandra.ClusterDeployment;

namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    [SetUpFixture]
    public class SingleCassandraNodeSetUpFixture
    {
        private const string cassandraTemplates = @"Assemblies\CassandraTemplate";

        internal static CassandraNode Node { get; private set; }

        [SetUp]
        public static void SetUp()
        {
            Node = new CassandraNode(FindCassandraTemplateDirectory(AppDomain.CurrentDomain.BaseDirectory))
                {
                    Name = "node_at_9360",
                    JmxPort = 7399,
                    GossipPort = 7400,
                    RpcPort = 9360,
                    CqlPort = 9343,
                    DeployDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\DeployedCassandra"),
                    ListenAddress = "127.0.0.1",
                    RpcAddress = "127.0.0.1",
                    SeedAddresses = new[] {"127.0.0.1"},
                    ClusterName = "test_cluster",
                };
            Node.Restart();
        }

        private static string FindCassandraTemplateDirectory(string currentDir)
        {
            if(currentDir == null)
                throw new Exception("Невозможно найти каталог с Cassandra-шаблонами");
            var cassandraTemplateDirectory = Path.Combine(currentDir, cassandraTemplates);
            return Directory.Exists(cassandraTemplateDirectory) ? cassandraTemplateDirectory : FindCassandraTemplateDirectory(Path.GetDirectoryName(currentDir));
        }

        [TearDown]
        public static void TearDown()
        {
            Node.Stop();
        }
    }
}
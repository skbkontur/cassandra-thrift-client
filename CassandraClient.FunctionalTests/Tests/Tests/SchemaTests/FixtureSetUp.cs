using System;
using System.IO;

using NUnit.Framework;

using SKBKontur.Cassandra.ClusterDeployment;

namespace SKBKontur.Cassandra.FunctionalTests.Tests.SchemaTests
{
    [SetUpFixture]
    public class StartSingleCassandraSetUp
    {
        internal static CassandraNode Node { get; private set; }
        private const string cassandraTemplates = @"Assemblies\CassandraTemplates";

        [SetUp]
        public static void SetUp()
        {
            Node = new CassandraNode(Path.Combine(FindCassandraTemplateDirectory(AppDomain.CurrentDomain.BaseDirectory), @"2.0"))
            {
                Name = "node_at_9360",
                JmxPort = 7399,
                GossipPort = 7400,
                RpcPort = 9360,
                CqlPort = 9343,
                DataBaseDirectory = @"../data/",
                DeployDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\Cassandra2.0"),
                ListenAddress = "127.0.0.1",
                RpsAddress = "0.0.0.0",
                SeedAddresses = new[] { "127.0.0.1" },
                InitialToken = "",
                ClusterName = "test_cluster"
            };
            Node.Restart();

        }

        private static string FindCassandraTemplateDirectory(string currentDir)
        {
            if (currentDir == null)
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
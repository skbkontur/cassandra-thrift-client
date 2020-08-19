using System;
using System.IO;

using NUnit.Framework;

using SkbKontur.Cassandra.Local;

namespace SkbKontur.Cassandra.ThriftClient.Tests.FunctionalTests.Tests
{
    [SetUpFixture]
    public class SingleCassandraNodeSetUpFixture
    {
        private const string cassandraTemplates = @"cassandra-local\cassandra\v3.11.x\";

        internal static LocalCassandraNode Node { get; private set; }

        [OneTimeSetUp]
        public static void SetUp()
        {
            var templateDirectory = FindCassandraTemplateDirectory(AppDomain.CurrentDomain.BaseDirectory);
            var deployDirectory = Path.Combine(Path.GetTempPath(), "deployed_cassandra_v3.11.x");
            Node = new LocalCassandraNode(templateDirectory, deployDirectory)
                {
                    RpcPort = 9360,
                    CqlPort = 9343,
                    JmxPort = 7399,
                    GossipPort = 7400
                };
            Node.Restart(timeout : TimeSpan.FromMinutes(1));
        }

        internal static string FindCassandraTemplateDirectory(string currentDir)
        {
            if (currentDir == null)
                throw new Exception("Невозможно найти каталог с Cassandra-шаблонами");
            var cassandraTemplateDirectory = Path.Combine(currentDir, cassandraTemplates);
            return Directory.Exists(cassandraTemplateDirectory) ? cassandraTemplateDirectory : FindCassandraTemplateDirectory(Path.GetDirectoryName(currentDir));
        }

        [OneTimeTearDown]
        public static void TearDown()
        {
            Node.Stop();
        }
    }
}
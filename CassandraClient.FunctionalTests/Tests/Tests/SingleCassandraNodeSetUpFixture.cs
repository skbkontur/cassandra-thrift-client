using System;
using System.IO;

using NUnit.Framework;

using SkbKontur.Cassandra.Local;

namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    [SetUpFixture]
    public class SingleCassandraNodeSetUpFixture
    {
        private const string cassandraTemplates = @"cassandra-local\cassandra\v2.2.x\";

        internal static LocalCassandraNode Node { get; private set; }

        [SetUp]
        public static void SetUp()
        {
            var templateDirectory = FindCassandraTemplateDirectory(AppDomain.CurrentDomain.BaseDirectory);
            var deployDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\DeployedCassandra");
            Node = new LocalCassandraNode(templateDirectory, deployDirectory)
                {
                    RpcPort = 9360,
                    CqlPort = 9343,
                    JmxPort = 7399,
                    GossipPort = 7400,
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
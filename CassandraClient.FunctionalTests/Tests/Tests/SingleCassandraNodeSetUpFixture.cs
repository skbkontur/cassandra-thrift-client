using System;
using System.IO;

using NUnit.Framework;

using SKBKontur.Cassandra.ClusterDeployment;
using SKBKontur.Cassandra.FunctionalTests.Utils;

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
            Node = new CassandraNode(
                FindCassandraTemplateDirectory(AppDomain.CurrentDomain.BaseDirectory),
                Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\DeployedCassandra"),
                new Log4NetWrapper(typeof(SingleCassandraNodeSetUpFixture))
            );
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
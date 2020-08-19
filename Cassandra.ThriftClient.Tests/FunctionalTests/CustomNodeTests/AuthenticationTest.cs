using System;
using System.IO;
using System.Threading;

using Cassandra;

using NUnit.Framework;

using SkbKontur.Cassandra.Local;
using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Clusters;
using SkbKontur.Cassandra.ThriftClient.Schema;
using SkbKontur.Cassandra.ThriftClient.Tests.FunctionalTests.Tests;
using SkbKontur.Cassandra.ThriftClient.Tests.FunctionalTests.Utils;

using Vostok.Logging.Abstractions;
using Logger = SkbKontur.Cassandra.ThriftClient.Tests.FunctionalTests.Utils.Logger;

namespace SkbKontur.Cassandra.ThriftClient.Tests.FunctionalTests.CustomNodeTests
{
    public class AuthenticationTest
    {
        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            var templateDirectory = SingleCassandraNodeSetUpFixture.FindCassandraTemplateDirectory(AppDomain.CurrentDomain.BaseDirectory);
            var deployDirectory = Path.Combine(Path.GetTempPath(), "deployed_cassandra_with_authentication_v3.11.x");
            node = new LocalCassandraNode(templateDirectory, deployDirectory, Authenticator.PasswordAuthenticator)
                {
                    LocalNodeName = "local_node_with_authentication",
                    RpcPort = 10360,
                    CqlPort = 10343,
                    JmxPort = 8399,
                    GossipPort = 8400
                };
            node.Restart(TimeSpan.FromMinutes(1));
            Thread.Sleep(TimeSpan.FromSeconds(30)); // required to initialize default superuser
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            node?.Stop(TimeSpan.FromMinutes(1));
        }

        [Test]
        public void TestAuthenticatedConnectionDefaultCredentials()
        {
            var settings = node.CreateSettings();
            settings.Credentials = new Credentials("cassandra", "cassandra");
            AssertAuthenticationSuccess(settings);
        }

        [Test]
        public void TestAuthenticatedConnectionNonDefaultCredentials()
        {
            var settings = node.CreateSettings();
            settings.Credentials = new Credentials("cassandra", "cassandra");
            var session = Cluster.Builder()
                                 .AddContactPoint("127.0.0.1")
                                 .WithPort(node.CqlPort)
                                 .WithCredentials("cassandra", "cassandra")
                                 .Build()
                                 .Connect();

            string user = "dba", password = "super";
            session.Execute($"CREATE ROLE {user} WITH SUPERUSER = true AND LOGIN = true AND PASSWORD = '{password}'");
            var customAuthenticationSettings = node.CreateSettings();
            customAuthenticationSettings.Credentials = new Credentials(user, password);
            AssertAuthenticationSuccess(customAuthenticationSettings);
        }

        private static void AssertAuthenticationSuccess(ICassandraClusterSettings settings)
        {
            var cluster = new CassandraCluster(settings, new SilentLog());
            var cassandraSchemaActualizer = new CassandraSchemaActualizer(cluster, null, Logger.Instance);
            Assert.DoesNotThrow(() =>
                                    cassandraSchemaActualizer.ActualizeKeyspaces(new[]
                                        {
                                            new KeyspaceSchema
                                                {
                                                    Name = Guid.NewGuid().ToString("N"),
                                                    Configuration = new KeyspaceConfiguration
                                                        {
                                                            ReplicationStrategy = SimpleReplicationStrategy.Create(1),
                                                            ColumnFamilies = new[]
                                                                {
                                                                    new ColumnFamily
                                                                        {
                                                                            Name = Constants.ColumnFamilyName
                                                                        },
                                                                    new ColumnFamily
                                                                        {
                                                                            Name = Constants.DefaultTtlColumnFamilyName,
                                                                            DefaultTtl = 1
                                                                        }
                                                                }
                                                        }
                                                }
                                        }, false));
        }

        private LocalCassandraNode node;
    }
}
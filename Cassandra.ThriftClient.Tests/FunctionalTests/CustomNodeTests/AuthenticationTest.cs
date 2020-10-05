using System;
using System.IO;
using System.Linq.Expressions;

using Cassandra;

using Moq;

using NUnit.Framework;

using SkbKontur.Cassandra.Local;
using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Clusters;
using SkbKontur.Cassandra.ThriftClient.Core.GenericPool.Exceptions;
using SkbKontur.Cassandra.ThriftClient.Tests.FunctionalTests.Tests;
using SkbKontur.Cassandra.ThriftClient.Tests.FunctionalTests.Utils;

using Vostok.Logging.Abstractions;

using AuthenticationException = Apache.Cassandra.AuthenticationException;

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

            Waiter.Wait(() =>
                {
                    try
                    {
                        using (var cluster = GetClusterBuilder().Build())
                        using (cluster.Connect())
                            return true;
                    }
                    catch
                    {
                        return false;
                    }
                }, timeout : TimeSpan.FromSeconds(30));
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            node?.Stop(TimeSpan.FromMinutes(1));
        }

        [Test]
        public void TestAuthenticatedConnectionDefaultCredentials()
        {
            Assert.DoesNotThrow(() => SomeActionThatRequiresAuthentication("cassandra", "cassandra"));
        }

        [Test]
        public void TestAuthenticatedConnectionNonDefaultCredentials()
        {
            string user = "dba", password = "super";
            using (var cluster = GetClusterBuilder().Build())
            using (var session = cluster.Connect())
                session.Execute($"CREATE ROLE {user} WITH SUPERUSER = true AND LOGIN = true AND PASSWORD = '{password}'");

            Assert.DoesNotThrow(() => SomeActionThatRequiresAuthentication(user, password));
        }

        [Test]
        public void TestNonAuthenticatedConnection()
        {
            var outerException = Assert.Throws<AllItemsIsDeadExceptions>(
                () => SomeActionThatRequiresAuthentication("non-existent", "weak_pa$$w0rd"));

            var authenticationException = outerException.InnerException as AuthenticationException;
            Assert.NotNull(authenticationException);
            Assert.AreEqual("Provided username non-existent and/or password are incorrect", authenticationException.Why);
        }

        [Test]
        public void TestThriftConnectionClosedAfterNonSuccessfulAuthentication()
        {
            var logger = new Mock<ILog>(MockBehavior.Strict);
            logger.Setup(l => l.ForContext(It.IsAny<string>()))
                  .Returns(logger.Object);

            logger.Setup(l => l.IsEnabledFor(It.IsAny<LogLevel>()))
                  .Returns((LogLevel level) => level == LogLevel.Error);

            Expression<Action<ILog>> logAuthFailSetup = l =>
                l.Log(It.Is<LogEvent>(
                    e => e.Exception is AuthenticationException
                         && e.MessageTemplate == "Error occured while opening thrift connection. Will try to close open transports. Failed action: {ActionName}."
                         && e.Properties != null
                         && e.Properties.ContainsKey("ActionName")
                         && e.Properties["ActionName"] as string == "login"));

            logger.Setup(logAuthFailSetup).Verifiable();
            Assert.Throws<AllItemsIsDeadExceptions>(() => SomeActionThatRequiresAuthentication("cassandra", "wrong_password", logger.Object));
            logger.Verify(logAuthFailSetup, Times.Exactly(2));
        }

        private void SomeActionThatRequiresAuthentication(string username, string password, ILog logger = null)
        {
            var settings = node.CreateSettings();
            settings.Credentials = new Credentials(username, password);
            using (var cluster = new CassandraCluster(settings, logger ?? new SilentLog()))
                cluster.RetrieveClusterConnection().RetrieveKeyspaces();
        }

        private Builder GetClusterBuilder()
        {
            return Cluster.Builder()
                          .AddContactPoint("127.0.0.1")
                          .WithPort(node.CqlPort)
                          .WithCredentials("cassandra", "cassandra");
        }

        private LocalCassandraNode node;
    }
}
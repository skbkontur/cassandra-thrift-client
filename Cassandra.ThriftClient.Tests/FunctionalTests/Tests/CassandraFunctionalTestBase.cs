using System;
using System.Text;
using System.Threading;

using Cassandra.ThriftClient.Tests.FunctionalTests.Utils;
using Cassandra.ThriftClient.Tests.FunctionalTests.Utils.ObjComparer;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Cassandra.CassandraClient.Exceptions;
using SKBKontur.Cassandra.CassandraClient.Scheme;

namespace Cassandra.ThriftClient.Tests.FunctionalTests.Tests
{
    public abstract class CassandraFunctionalTestBase
    {
        [SetUp]
        public virtual void SetUp()
        {
            KeyspaceName = $"TestKeyspace_{Guid.NewGuid():N}";
            var cassandraClusterSettings = SingleCassandraNodeSetUpFixture.Node.CreateSettings();
            cassandraClusterSettings.AllowNullTimestamp = true;
            cassandraClusterSettings.ReadConsistencyLevel = ConsistencyLevel.ALL;
            cassandraClusterSettings.WriteConsistencyLevel = ConsistencyLevel.ALL;
            cassandraCluster = new CassandraCluster(cassandraClusterSettings, Logger.Instance);
            columnFamilyConnection = cassandraCluster.RetrieveColumnFamilyConnection(KeyspaceName, Constants.ColumnFamilyName);
            columnFamilyConnectionDefaultTtl = cassandraCluster.RetrieveColumnFamilyConnection(KeyspaceName, Constants.DefaultTtlColumnFamilyName);
            ClearKeyspacesOnce();
            cassandraCluster.ActualizeKeyspaces(new[]
                {
                    new KeyspaceScheme
                        {
                            Name = KeyspaceName,
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
                });
        }

        [TearDown]
        public virtual void TearDown()
        {
        }

        protected string KeyspaceName { get; private set; }

        private void ClearKeyspacesOnce()
        {
            if (keyspacesDeleted) return;
            var clusterConnection = cassandraCluster.RetrieveClusterConnection();
            var result = clusterConnection.RetrieveKeyspaces();
            foreach (var keyspace in result)
                cassandraCluster.RetrieveClusterConnection().RemoveKeyspace(keyspace.Name);
            keyspacesDeleted = true;
        }

        protected static Column ToColumn(string columnName, string columnValue, long? timestamp = null, int? ttl = null)
        {
            return new Column
                {
                    Name = columnName,
                    Value = ToBytes(columnValue),
                    Timestamp = timestamp,
                    TTL = ttl
                };
        }

        private static byte[] ToBytes(string str)
        {
            return Encoding.UTF8.GetBytes(str);
        }

        protected static string ToString(byte[] bytes)
        {
            return Encoding.UTF8.GetString(bytes);
        }

        protected void Check(string key, string columnName, string columnValue, long? timestamp = null, int? ttl = null, IColumnFamilyConnection cfc = null)
        {
            var connection = cfc ?? columnFamilyConnection;
            Assert.IsTrue(connection.TryGetColumn(key, columnName, out var tryGetResult));
            var result = connection.GetColumn(key, columnName);
            tryGetResult.AssertEqualsTo(result);
            Assert.AreEqual(columnName, result.Name);
            Assert.AreEqual(columnValue, ToString(result.Value));
            if (timestamp == null)
                Assert.IsNotNull(result.Timestamp);
            else
                Assert.AreEqual(timestamp, result.Timestamp);
            Assert.AreEqual(ttl, result.TTL);
        }

        protected void CheckNotFound(string key, string columnName, IColumnFamilyConnection cfc = null)
        {
            var connection = cfc ?? columnFamilyConnection;
            RunMethodWithException<ColumnIsNotFoundException>(() => connection.GetColumn(key, columnName));
            Assert.IsFalse(connection.TryGetColumn(key, columnName, out _));
        }

        private static void RunMethodWithException<TE>(Action method) where TE : Exception
        {
            RunMethodWithException(method, (Action<TE>)null);
        }

        private static void RunMethodWithException<TE>(Action method, Action<TE> exceptionCheckDelegate)
            where TE : Exception
        {
            if (typeof(TE) == typeof(Exception) || typeof(TE) == typeof(AssertionException))
                Assert.Fail("использование типа {0} запрещено", typeof(TE));
            try
            {
                method();
            }
            catch (TE e)
            {
                if (e is ThreadAbortException)
                    Thread.ResetAbort();
                exceptionCheckDelegate?.Invoke(e);
                return;
            }
            Assert.Fail("Method didn't thrown expected exception " + typeof(TE));
        }

        protected ICassandraCluster cassandraCluster;
        protected IColumnFamilyConnection columnFamilyConnection;
        private static bool keyspacesDeleted;
        protected IColumnFamilyConnection columnFamilyConnectionDefaultTtl;
    }
}
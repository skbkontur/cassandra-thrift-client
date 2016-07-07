using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

using Cassandra.Tests;

using GroboContainer.Core;
using GroboContainer.Impl;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Cassandra.CassandraClient.Exceptions;
using SKBKontur.Cassandra.CassandraClient.Scheme;
using SKBKontur.Cassandra.FunctionalTests.Utils;

namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    public abstract class CassandraFunctionalTestBase : TestBase
    {
        protected ICassandraCluster cassandraCluster;
        protected IColumnFamilyConnection columnFamilyConnection;
        private static bool keyspacesDeleted;
        protected IColumnFamilyConnection columnFamilyConnectionDefaultTtl;
        protected string KeyspaceName { get; private set; }

        public override void SetUp()
        {
            base.SetUp();
            KeyspaceName = "TestKeyspace_" + Guid.NewGuid().ToString("N");
            var assemblies = new List<Assembly>(AssembliesLoader.Load()) {Assembly.GetExecutingAssembly()};
            var container = new Container(new ContainerConfiguration(assemblies));
            cassandraCluster = container.Get<ICassandraCluster>();
            columnFamilyConnection = cassandraCluster.RetrieveColumnFamilyConnection(KeyspaceName, Constants.ColumnFamilyName);
            columnFamilyConnectionDefaultTtl = cassandraCluster.RetrieveColumnFamilyConnection(KeyspaceName, Constants.DefaultTtlColumnFamilyName);
            ServiceUtils.ConfugureLog4Net(AppDomain.CurrentDomain.BaseDirectory);
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

        private void ClearKeyspacesOnce()
        {
            if(keyspacesDeleted) return;
            var clusterConnection = cassandraCluster.RetrieveClusterConnection();
            var result = clusterConnection.RetrieveKeyspaces();
            foreach(var keyspace in result)
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

        protected static byte[] ToBytes(string str)
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
            Column tryGetResult;
            Assert.IsTrue(connection.TryGetColumn(key, columnName, out tryGetResult));
            Column result = connection.GetColumn(key, columnName);
            tryGetResult.AssertEqualsTo(result);
            Assert.AreEqual(columnName, result.Name);
            Assert.AreEqual(columnValue, ToString(result.Value));
            if(timestamp == null)
                Assert.IsNotNull(result.Timestamp);
            else
                Assert.AreEqual(timestamp, result.Timestamp);
            Assert.AreEqual(ttl, result.TTL);
        }

        protected void CheckNotFound(string key, string columnName, IColumnFamilyConnection cfc = null)
        {
            var connection = cfc ?? columnFamilyConnection;
            RunMethodWithException<ColumnIsNotFoundException>(
                () => connection.GetColumn(key, columnName));
            Column column;
            Assert.IsFalse(connection.TryGetColumn(key, columnName, out column));
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;

namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    public class CassandraClient : ICassandraClient
    {
        public CassandraClient(ICassandraCluster cassandraCluster)
        {
            this.cassandraCluster = cassandraCluster;
        }

        public void RemoveAllKeyspaces()
        {
            //TODO хак из-за бага в кассандре.Убрать с версии 0.7.5
            try
            {
                UnsafeRemoveKeyspaces();
            }
            catch(Exception)
            {
                UnsafeRemoveKeyspaces();
            }
        }

        public void AddKeyspace(string keySpaceName, string columnFamilyName)
        {
            var clusterConnection = cassandraCluster.RetrieveClusterConnection();
            clusterConnection.AddKeyspace(new Keyspace
                {
                    ColumnFamilies = new Dictionary<string, ColumnFamily>
                        {
                            {
                                columnFamilyName, new ColumnFamily
                                    {
                                        Name = columnFamilyName
                                    }
                                }
                        },
                    Name = keySpaceName,
                    ReplicaPlacementStrategy = "org.apache.cassandra.locator.SimpleStrategy",
                    ReplicationFactor = 1
                });
        }

        public bool TryGetColumn(string keySpaceName, string columnFamilyName, string key, string columnName,
                                 out Column result)
        {
            var columnFamilyConnection = cassandraCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName);
            return columnFamilyConnection.TryGetColumn(key, columnName, out result);
        }

        public Column GetColumn(string keySpaceName, string columnFamilyName, string key, string columnName)
        {
            var columnFamilyConnection = cassandraCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName);
            return columnFamilyConnection.GetColumn(key, columnName);
        }

        public void DeleteColumn(string keySpaceName, string columnFamilyName, string key, string columnName)
        {
            var columnFamilyConnection = cassandraCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName);
            columnFamilyConnection.DeleteBatch(key, new[] {columnName});
        }

        public void Add(string keySpaceName, string columnFamilyName, string key, string columnName, byte[] columnValue,
                        long? timestamp, int? ttl)
        {
            var columnFamilyConnection = cassandraCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName);
            columnFamilyConnection.AddColumn(key, new Column
                {
                    Name = columnName,
                    Value = columnValue,
                    Timestamp = timestamp,
                    TTL = ttl
                });
        }

        public void AddBatch(string keySpaceName, string columnFamilyName, string key, Column[] columns)
        {
            var columnFamilyConnection = cassandraCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName);
            columnFamilyConnection.AddBatch(key, columns);
        }

        public void DeleteBatch(string keySpaceName, string columnFamilyName, string key, IEnumerable<string> columnNames, long? timestamp = null)
        {
            var columnFamilyConnection = cassandraCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName);
            columnFamilyConnection.DeleteBatch(key, columnNames, timestamp);
        }

        public Column[] GetRow(string keySpaceName, string columnFamilyName, string key, int count, string startColumnName)
        {
            var columnFamilyConnection = cassandraCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName);
            return columnFamilyConnection.GetColumns(key, startColumnName, count);
        }

        public Column[] GetRow(string keySpaceName, string columnFamilyName, string key)
        {
            var columnFamilyConnection = cassandraCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName);
            return columnFamilyConnection.GetRow(key, 10).ToArray();
        }

        public string[] GetKeys(string keySpaceName, string columnFamilyName)
        {
            var columnFamilyConnection = cassandraCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName);
            return columnFamilyConnection.GetKeys(10).ToArray();
        }

        public int GetCount(string keySpaceName, string columnFamilyName, string key)
        {
            var columnFamilyConnection = cassandraCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName);
            return columnFamilyConnection.GetCount(key);
        }

        public Dictionary<string, int> GetCounts(string keySpaceName, string columnFamilyName, IEnumerable<string> keys)
        {
            Dictionary<string, int> result;
            var columnFamilyConnection = cassandraCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName);
            result = columnFamilyConnection.GetCounts(keys);
            return result;
        }

        public void CheckConnections()
        {
            cassandraCluster.CheckConnections();
        }

        private void UnsafeRemoveKeyspaces()
        {
            var clusterConnection = cassandraCluster.RetrieveClusterConnection();
            var result = clusterConnection.RetrieveKeyspaces();
            foreach(var keyspace in result)
                RemoveKeyspace(keyspace.Name);
        }

        private void RemoveKeyspace(string keyspaceName)
        {
            var clusterConnection = cassandraCluster.RetrieveClusterConnection();
            clusterConnection.RemoveKeyspace(keyspaceName);
        }

        private readonly ICassandraCluster cassandraCluster;
    }
}
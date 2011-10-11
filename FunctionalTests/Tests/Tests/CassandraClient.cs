using System;
using System.Collections.Generic;
using System.Linq;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;

namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    public class CassandraClient : ICassandraClient
    {
        public CassandraClient(ICassandraCluster cassandraCluster)
        {
            this.cassandraCluster = cassandraCluster;
        }

        #region ICassandraClient Members

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
            using(IClusterConnection clusterConnection = cassandraCluster.RetrieveClusterConnection())
            {
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
        }

        public bool TryGetColumn(string keySpaceName, string columnFamilyName, string key, string columnName,
                                 out Column result)
        {
            bool returnResult;
            using(
                IColumnFamilyConnection columnFamilyConnection =
                    cassandraCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName))
                returnResult = columnFamilyConnection.TryGetColumn(key, columnName, out result);
            return returnResult;
        }

        public Column GetColumn(string keySpaceName, string columnFamilyName, string key, string columnName)
        {
            Column result;
            using(
                IColumnFamilyConnection columnFamilyConnection =
                    cassandraCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName))
                result = columnFamilyConnection.GetColumn(key, columnName);
            return result;
        }

        public void DeleteColumn(string keySpaceName, string columnFamilyName, string key, string columnName)
        {
            using(IColumnFamilyConnection columnFamilyConnection = cassandraCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName))
                columnFamilyConnection.DeleteBatch(key, new[] {columnName});
        }

        private void UnsafeRemoveKeyspaces()
        {
            IList<Keyspace> result;
            using(IClusterConnection clusterConnection = cassandraCluster.RetrieveClusterConnection())
                result = clusterConnection.RetrieveKeyspaces();

            foreach(var keyspace in result)
                RemoveKeyspace(keyspace.Name);
        }

        #endregion

        public void Add(string keySpaceName, string columnFamilyName, string key, string columnName, byte[] columnValue,
                        long? timestamp, int? ttl)
        {
            using(IColumnFamilyConnection columnFamilyConnection =
                cassandraCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName))
            {
                columnFamilyConnection.AddColumn(key, new Column
                    {
                        Name = columnName,
                        Value = columnValue,
                        Timestamp = timestamp,
                        TTL = ttl
                    });
            }
        }

        public void AddBatch(string keySpaceName, string columnFamilyName, string key, Column[] columns)
        {
            using(IColumnFamilyConnection columnFamilyConnection =
                cassandraCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName))
                columnFamilyConnection.AddBatch(key, columns);
        }

        public void DeleteBatch(string keySpaceName, string columnFamilyName, string key, IEnumerable<string> columnNames, long? timestamp = null)
        {
            using(IColumnFamilyConnection columnFamilyConnection =
                cassandraCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName))
                columnFamilyConnection.DeleteBatch(key, columnNames, timestamp);
        }

        public Column[] GetRow(string keySpaceName, string columnFamilyName, string key, int count, string startColumnName)
        {
            Column[] result;
            using(IColumnFamilyConnection columnFamilyConnection =
                cassandraCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName))
                result = columnFamilyConnection.GetRow(key, startColumnName, count);
            return result;
        }

        public Column[] GetRow(string keySpaceName, string columnFamilyName, string key)
        {
            IEnumerable<Column> result;
            using(IColumnFamilyConnection columnFamilyConnection =
                cassandraCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName))
                result = columnFamilyConnection.GetRow(key, 10);
            return result.ToArray();
        }

        public string[] GetKeys(string keySpaceName, string columnFamilyName)
        {
            IEnumerable<string> result;
            using(IColumnFamilyConnection columnFamilyConnection =
                cassandraCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName))
                result = columnFamilyConnection.GetKeys(10);
            return result.ToArray();
        }

        public int GetCount(string keySpaceName, string columnFamilyName, string key)
        {
            int result;
            using (IColumnFamilyConnection columnFamilyConnection =
                cassandraCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName))
                result = columnFamilyConnection.GetCount(key);
            return result;
        }

        public Dictionary<string, int> GetCounts(string keySpaceName, string columnFamilyName, IEnumerable<string> keys)
        {
            Dictionary<string, int> result;
            using (IColumnFamilyConnection columnFamilyConnection =
                cassandraCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName))
                result = columnFamilyConnection.GetCounts(keys);
            return result;
        }

        private void RemoveKeyspace(string keyspaceName)
        {
            using(IClusterConnection clusterConnection = cassandraCluster.RetrieveClusterConnection())
                clusterConnection.RemoveKeyspace(keyspaceName);
        }

        private readonly ICassandraCluster cassandraCluster;
    }
}
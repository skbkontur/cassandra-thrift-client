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
            var columnFamilyConnection = cassandraCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName);
            return columnFamilyConnection.GetCounts(keys);
        }

        private readonly ICassandraCluster cassandraCluster;
    }
}
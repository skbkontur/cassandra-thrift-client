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

        private readonly ICassandraCluster cassandraCluster;
    }
}
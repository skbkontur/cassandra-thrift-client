using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Cassandra.CassandraClient.Helpers;

namespace SKBKontur.Cassandra.StorageCore
{
    public abstract class OneValueRepository : IOneValueRepository
    {
        protected OneValueRepository(ICassandraCluster cassandraCluster, ICassandraCoreSettings cassandraCoreSettings, string columnFamilyName)
        {
            connection = cassandraCluster.RetrieveColumnFamilyConnection(cassandraCoreSettings.KeyspaceName, columnFamilyName);
        }

        public void Write(string value)
        {
            connection.AddColumn("Key", new Column {Name = "Value", Value = StringHelpers.StringToBytes(value)});
        }

        public string TryRead()
        {
            Column column;
            return !connection.TryGetColumn("Key", "Value", out column) ? null : StringHelpers.BytesToString(column.Value);
        }

        private readonly IColumnFamilyConnection connection;
    }
}
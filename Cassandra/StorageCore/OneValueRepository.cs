using CassandraClient.Abstractions;
using CassandraClient.Clusters;
using CassandraClient.Helpers;

namespace StorageCore
{
    public abstract class OneValueRepository : IOneValueRepository
    {
        protected OneValueRepository(ICassandraCluster cassandraCluster, ICassandraCoreSettings cassandraCoreSettings, string columnFamilyName)
        {
            this.cassandraCluster = cassandraCluster;
            this.cassandraCoreSettings = cassandraCoreSettings;
            this.columnFamilyName = columnFamilyName;
        }

        public void Write(string value)
        {
            using(var conn = cassandraCluster.RetrieveColumnFamilyConnection(cassandraCoreSettings.KeyspaceName, columnFamilyName))
                conn.AddColumn("Key", new Column {Name = "Value", Value = StringHelpers.StringToBytes(value)});
        }

        public string TryRead()
        {
            Column column;
            using(var conn = cassandraCluster.RetrieveColumnFamilyConnection(cassandraCoreSettings.KeyspaceName, columnFamilyName))
                return !conn.TryGetColumn("Key", "Value", out column) ? null : StringHelpers.BytesToString(column.Value);
        }

        private readonly ICassandraCluster cassandraCluster;
        private readonly ICassandraCoreSettings cassandraCoreSettings;
        private readonly string columnFamilyName;
    }
}
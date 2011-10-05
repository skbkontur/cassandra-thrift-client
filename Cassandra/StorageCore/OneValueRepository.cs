using CassandraClient.Abstractions;
using CassandraClient.Clusters;

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
                conn.AddColumn("Key", new Column {Name = "Value", Value = CassandraStringHelpers.StringToBytes(value)});
        }

        public string TryRead()
        {
            Column column;
            using(var conn = cassandraCluster.RetrieveColumnFamilyConnection(cassandraCoreSettings.KeyspaceName, columnFamilyName))
                return !conn.TryGetColumn("Key", "Value", out column) ? null : CassandraStringHelpers.BytesToString(column.Value);
        }

        private readonly ICassandraCluster cassandraCluster;
        private readonly ICassandraCoreSettings cassandraCoreSettings;
        private readonly string columnFamilyName;
    }
}
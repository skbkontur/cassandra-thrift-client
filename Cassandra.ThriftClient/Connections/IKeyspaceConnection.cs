using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Connections
{
    public interface IKeyspaceConnection
    {
        void RemoveColumnFamily(string columnFamily);
        void AddColumnFamily(string columnFamilyName);
        void UpdateColumnFamily(ColumnFamily columnFamily);
        void AddColumnFamily(ColumnFamily columnFamily);
        Keyspace DescribeKeyspace();
    }
}
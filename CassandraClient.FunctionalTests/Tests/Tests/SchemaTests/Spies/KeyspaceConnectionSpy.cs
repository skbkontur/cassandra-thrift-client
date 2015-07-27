using GroboContainer.Infection;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Connections;

namespace SKBKontur.Cassandra.FunctionalTests.Tests.SchemaTests.Spies
{
    [IgnoredImplementation]
    public class KeyspaceConnectionSpy : IKeyspaceConnection
    {
        public KeyspaceConnectionSpy(IKeyspaceConnection innerConnection)
        {
            this.innerConnection = innerConnection;
        }

        public void RemoveColumnFamily(string columnFamily)
        {
            innerConnection.RemoveColumnFamily(columnFamily);
        }

        public void AddColumnFamily(string columnFamilyName)
        {
            innerConnection.AddColumnFamily(columnFamilyName);
        }

        public void UpdateColumnFamily(ColumnFamily columnFamily)
        {
            UpdateColumnFamilyInvokeCount++;
            innerConnection.UpdateColumnFamily(columnFamily);
        }

        public void AddColumnFamily(ColumnFamily columnFamily)
        {
            innerConnection.AddColumnFamily(columnFamily);
        }

        public Keyspace DescribeKeyspace()
        {
            return innerConnection.DescribeKeyspace();
        }

        public int UpdateColumnFamilyInvokeCount { get; private set; }
        private readonly IKeyspaceConnection innerConnection;
    }
}
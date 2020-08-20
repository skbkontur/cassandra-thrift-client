using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Connections;

namespace SkbKontur.Cassandra.ThriftClient.Tests.FunctionalTests.Tests.SchemaTests.Spies
{
    public class KeyspaceConnectionSpy : IKeyspaceConnection
    {
        public KeyspaceConnectionSpy(IKeyspaceConnection innerConnection)
        {
            this.innerConnection = innerConnection;
        }

        public int UpdateColumnFamilyInvokeCount { get; private set; }

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

        private readonly IKeyspaceConnection innerConnection;
    }
}
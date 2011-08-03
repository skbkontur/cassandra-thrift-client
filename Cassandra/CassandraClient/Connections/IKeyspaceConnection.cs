using System;

using CassandraClient.Abstractions;

namespace CassandraClient.Connections
{
    public interface IKeyspaceConnection : IDisposable
    {
        void RemoveColumnFamily(string columnFamily);
        void AddColumnFamily(string columnFamilyName);
        void UpdateColumnFamily(ColumnFamily columnFamily);
        void AddColumnFamily(ColumnFamily columnFamily);
        Keyspace DescribeKeyspace();
    }
}
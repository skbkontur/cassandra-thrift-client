using System;
using System.Collections.Generic;

using CassandraClient.Abstractions;

namespace CassandraClient.Connections
{
    public interface IClusterConnection : IDisposable
    {
        IList<Keyspace> RetrieveKeyspaces();
        void AddKeyspace(Keyspace keyspace);
        void RemoveKeyspace(string keyspace);
        void RemoveColumnFamily(string columnFamily);
        void AddColumnFamily(string keyspace, string columnFamilyName);
        void UpdateColumnFamily(ColumnFamily columnFamily);
        Keyspace DescribeKeyspace(string keyspace);
        void AddColumnFamily(ColumnFamily columnFamily);
    }
}
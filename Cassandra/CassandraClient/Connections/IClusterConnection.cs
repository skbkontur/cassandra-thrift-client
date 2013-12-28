using System.Collections.Generic;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Connections
{
    public interface IClusterConnection
    {
        IList<Keyspace> RetrieveKeyspaces();
        void AddKeyspace(Keyspace keyspace);
        void UpdateKeyspace(Keyspace keyspace);
        void RemoveKeyspace(string keyspace);
        string DescribeVersion();
    }
}
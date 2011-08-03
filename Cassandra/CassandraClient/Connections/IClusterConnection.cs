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
    }
}
using System;
using System.Collections.Generic;

using SkbKontur.Cassandra.ThriftClient.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Connections
{
    public interface IClusterConnection
    {
        IList<Keyspace> RetrieveKeyspaces();
        void AddKeyspace(Keyspace keyspace);
        void UpdateKeyspace(Keyspace keyspace);
        void RemoveKeyspace(string keyspace);
        string DescribeVersion();
        void WaitUntilSchemaAgreementIsReached(TimeSpan timeout);
    }
}
using System;
using System.Collections.Generic;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Core.Pools;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    public interface ICommandExecuter : IDisposable
    {
        void Execute(ICommand command);
        Dictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge> GetKnowledges();
        void CheckConnections();
    }
}
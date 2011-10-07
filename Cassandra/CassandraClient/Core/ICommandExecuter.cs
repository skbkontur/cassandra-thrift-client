using System.Collections.Generic;

using CassandraClient.Abstractions;
using CassandraClient.Core.Pools;

namespace CassandraClient.Core
{
    public interface ICommandExecuter
    {
        void Execute(ICommand command);
        Dictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge> GetKnowledges();
    }
}
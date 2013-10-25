using System;
using System.Net;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Core.GenericPool;
using SKBKontur.Cassandra.CassandraClient.Core.Pools;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    public interface IThriftConnectionInPoolWrapper : IDisposable, IPoolKeyContainer<ConnectionKey, IPEndPoint>, ILiveness
    {
        void ExecuteCommand(ICommand command);
    }
}
using System;
using System.Net;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Core.Pools;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    public class ThriftConnectionInPoolWrapper : IThriftConnection
    {
        public ThriftConnectionInPoolWrapper(int timeout, IPEndPoint ipEndPoint, string keyspaceName)
        {
            thriftConnection = new ThriftConnection(timeout, ipEndPoint, keyspaceName);
            ReplicaKey = ipEndPoint;
            KeyspaceName = keyspaceName;
        }

        public void Dispose()
        {
            thriftConnection.Dispose();
        }

        public bool IsAlive { get { return thriftConnection.IsAlive; } }
        public string KeyspaceName { get; private set; }
        public IPEndPoint ReplicaKey { get; private set; }

        public void ExecuteCommand(ICommand command)
        {
            thriftConnection.ExecuteCommand(command);
        }

        public DateTime CreationDateTime { get { return thriftConnection.CreationDateTime; } }

        private readonly ThriftConnection thriftConnection;
    }
}
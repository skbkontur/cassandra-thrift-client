using System;
using System.Net;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

using Vostok.Logging;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    internal class ThriftConnectionInPoolWrapper : IThriftConnection
    {
        public ThriftConnectionInPoolWrapper(int timeout, IPEndPoint ipEndPoint, string keyspaceName, ILog logger)
        {
            thriftConnection = new ThriftConnection(timeout, ipEndPoint, keyspaceName, logger);
            ReplicaKey = ipEndPoint;
            KeyspaceName = keyspaceName;
        }

        public void Dispose()
        {
            thriftConnection.Dispose();
        }

        public bool IsAlive { get { return thriftConnection.IsAlive; } }

        public void ExecuteCommand(ICommand command)
        {
            thriftConnection.ExecuteCommand(command);
        }

        public DateTime CreationDateTime { get { return thriftConnection.CreationDateTime; } }

        public override string ToString()
        {
            return string.Format("ThriftConnectionInPoolWrapper[KeyspaceName: {0}, NodeEndPoint: {1}]", KeyspaceName, ReplicaKey);
        }

        public string KeyspaceName { get; private set; }
        public IPEndPoint ReplicaKey { get; private set; }

        private readonly ThriftConnection thriftConnection;
    }
}
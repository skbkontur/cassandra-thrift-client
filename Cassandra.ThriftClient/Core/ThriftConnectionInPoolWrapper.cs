using System.Net;

using SkbKontur.Cassandra.ThriftClient.Abstractions;

using Vostok.Logging.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Core
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

        public bool IsAlive => thriftConnection.IsAlive;

        public void ExecuteCommand(ICommand command)
        {
            thriftConnection.ExecuteCommand(command);
        }

        public override string ToString()
        {
            return $"ThriftConnectionInPoolWrapper[KeyspaceName: {KeyspaceName}, NodeEndPoint: {ReplicaKey}]";
        }

        public string KeyspaceName { get; }
        public IPEndPoint ReplicaKey { get; }

        private readonly ThriftConnection thriftConnection;
    }
}
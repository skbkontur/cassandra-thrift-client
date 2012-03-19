using System;
using System.Net;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Core.Pools;
using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    public class PooledThriftConnection : IPooledThriftConnection
    {
        public PooledThriftConnection(IKeyspaceConnectionPool connectionPool, int timeout, IPEndPoint ipEndPoint, string keyspaceName, ICassandraLogManager logManager)
        {
            thriftConnection = new ThriftConnection(timeout, ipEndPoint, keyspaceName, logManager);
            this.connectionPool = connectionPool;
            Id = Guid.NewGuid();
        }

        public void Dispose()
        {
            connectionPool.ReleaseConnection(this);
        }

        public void ExecuteCommand(ICommand command, ICassandraLogger logger)
        {
            thriftConnection.ExecuteCommand(command, logger);
        }

        public bool Ping()
        {
            return thriftConnection.Ping();
        }

        public void Kill()
        {
            try
            {
                thriftConnection.Dispose();
            }
            catch
            {
            }
        }

        public Guid Id { get; private set; }
        public bool IsAlive { get { return thriftConnection.IsAlive; } set { thriftConnection.IsAlive = value; } }

        public override string ToString()
        {
            return string.Format("PooledThriftConnection[ThriftConnection='{0}' Id='{1}']", thriftConnection, Id);
        }

        private readonly IKeyspaceConnectionPool connectionPool;
        private readonly ThriftConnection thriftConnection;
    }
}
using System;
using System.Net;

using CassandraClient.Core.Pools;

namespace CassandraClient.Core
{
    public class PooledThriftConnection : ThriftConnection, IPooledThriftConnection
    {
        public PooledThriftConnection(IKeyspaceConnectionPool connectionPool, int timeout, IPEndPoint ipEndPoint, string keyspaceName)
            : base(timeout, ipEndPoint, keyspaceName)
        {
            this.connectionPool = connectionPool;
            Id = Guid.NewGuid();
        }

        public override void Dispose()
        {
            connectionPool.ReleaseConnection(this);
        }

        public Guid Id { get; private set; }

        public override string ToString()
        {
            return string.Format("ThriftConnection[EndPoint='{0}' KeyspaceName='{1}' Id='{2}']", IpEndPoint, KeyspaceName, Id);
        }

        private readonly IKeyspaceConnectionPool connectionPool;
    }
}
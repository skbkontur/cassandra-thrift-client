using System;
using System.Net;

using CassandraClient.Core.Pools;

namespace CassandraClient.Core
{
    public class PooledThriftConnection : ThriftConnection
    {
        public PooledThriftConnection(IKeyspaceConnectionPool connectionPool, int timeout, IPEndPoint ipEndPoint, string keyspaceName)
            : base(timeout, ipEndPoint, keyspaceName)
        {
            this.connectionPool = connectionPool;
            Id = Guid.NewGuid();
            KeyspaceName = keyspaceName;
        }

        public override void Dispose()
        {
            connectionPool.ReleaseConnection(this);
        }

        public override string ToString()
        {
            return string.Format("ThriftConnection[EndPoint='{0}' KeyspaceName='{1}' Id='{2}']", IpEndPoint, KeyspaceName, Id);
        }

        public string KeyspaceName { get; private set; }
        public Guid Id { get; private set; }

        private readonly IKeyspaceConnectionPool connectionPool;
    }
}
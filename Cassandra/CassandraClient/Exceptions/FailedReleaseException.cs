using System;

using CassandraClient.Core;

namespace CassandraClient.Exceptions
{
    public class FailedReleaseException : Exception
    {
        public FailedReleaseException(PooledThriftConnection connection) : base(connection.ToString())
        {
        }
    }
}
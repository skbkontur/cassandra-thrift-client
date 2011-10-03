using System;

using CassandraClient.Core;

namespace CassandraClient.Exceptions
{
    public class FailedReleaseException : Exception
    {
        public FailedReleaseException(IPooledThriftConnection connection)
            : base(connection.ToString())
        {
        }
    }
}
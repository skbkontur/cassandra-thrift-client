using System;

namespace SKBKontur.Cassandra.CassandraClient.Core.GenericPool.Exceptions
{
    public class IdleTimeoutToSmallException : ArgumentException
    {
        public IdleTimeoutToSmallException(string message, string paramName)
            : base(message, paramName)
        {
        }
    }
}
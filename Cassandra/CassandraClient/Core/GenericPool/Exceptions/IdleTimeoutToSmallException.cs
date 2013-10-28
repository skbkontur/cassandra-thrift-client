using System;

namespace SKBKontur.Cassandra.CassandraClient.Core.GenericPool.Exceptions
{
    internal class IdleTimeoutToSmallException : ArgumentException
    {
        public IdleTimeoutToSmallException(string message, string paramName)
            : base(message, paramName)
        {
        }
    }
}
using System;

namespace SkbKontur.Cassandra.ThriftClient.Core.GenericPool.Exceptions
{
    internal class IdleTimeoutToSmallException : ArgumentException
    {
        public IdleTimeoutToSmallException(string message, string paramName)
            : base(message, paramName)
        {
        }
    }
}
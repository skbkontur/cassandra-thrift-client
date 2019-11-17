using System;

namespace SkbKontur.Cassandra.ThriftClient.Core.GenericPool.Exceptions
{
    internal class InvalidPoolKeyException : Exception
    {
        public InvalidPoolKeyException(string message)
            : base(message)
        {
        }
    }
}
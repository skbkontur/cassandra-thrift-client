using System;

namespace SkbKontur.Cassandra.ThriftClient.Core.GenericPool.Exceptions
{
    internal class EmptyPoolException : Exception
    {
        public EmptyPoolException(string message)
            : base(message)
        {
        }
    }
}
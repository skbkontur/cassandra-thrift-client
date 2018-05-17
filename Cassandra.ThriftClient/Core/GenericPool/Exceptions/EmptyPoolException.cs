using System;

namespace SKBKontur.Cassandra.CassandraClient.Core.GenericPool.Exceptions
{
    internal class EmptyPoolException : Exception
    {
        public EmptyPoolException(string message)
            : base(message)
        {
        }
    }
}
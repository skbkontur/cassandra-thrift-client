using System;

namespace SKBKontur.Cassandra.CassandraClient.Core.GenericPool.Exceptions
{
    internal class InvalidPoolKeyException : Exception
    {
        public InvalidPoolKeyException(string message)
            : base(message)
        {
        }
    }
}
using System;

namespace SKBKontur.Cassandra.CassandraClient.Core.GenericPool.Exceptions
{
    public class InvalidPoolKeyException : Exception
    {
        public InvalidPoolKeyException(string message)
            : base(message)
        {
        }
    }
}
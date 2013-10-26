using System;

namespace SKBKontur.Cassandra.CassandraClient.Core.GenericPool.Exceptions
{
    public class EmptyPoolException : Exception
    {
        public EmptyPoolException(string message)
            : base(message)
        {
            
        }
    }
}
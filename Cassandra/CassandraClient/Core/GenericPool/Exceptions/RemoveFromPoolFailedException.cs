using System;

namespace SKBKontur.Cassandra.CassandraClient.Core.GenericPool.Exceptions
{
    public class RemoveFromPoolFailedException : Exception
    {
        public RemoveFromPoolFailedException(string message)
            : base(message)
        {
        }
    }
}
using System;

namespace SKBKontur.Cassandra.CassandraClient.Core.GenericPool.Exceptions
{
    public class FailedReleaseItemException : Exception
    {
        public FailedReleaseItemException(string message)
            : base(message)
        {
        }
    }
}
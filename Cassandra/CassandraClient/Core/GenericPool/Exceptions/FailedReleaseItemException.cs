using System;

namespace SKBKontur.Cassandra.CassandraClient.Core.GenericPool.Exceptions
{
    internal class FailedReleaseItemException : Exception
    {
        public FailedReleaseItemException(string message)
            : base(message)
        {
        }
    }
}
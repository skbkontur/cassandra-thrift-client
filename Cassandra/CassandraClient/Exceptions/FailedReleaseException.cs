using System;

using SKBKontur.Cassandra.CassandraClient.Core;

namespace SKBKontur.Cassandra.CassandraClient.Exceptions
{
    public class FailedReleaseException : Exception
    {
        public FailedReleaseException(string message)
            : base(message)
        {
        }
    }
}
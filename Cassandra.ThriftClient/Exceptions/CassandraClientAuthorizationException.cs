using System;

namespace SKBKontur.Cassandra.CassandraClient.Exceptions
{
    public class CassandraClientAuthorizationException : CassandraClientException
    {
        public CassandraClientAuthorizationException(string message)
            : base(message)
        {
        }

        public CassandraClientAuthorizationException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public override bool IsCorruptConnection { get { return false; } }
        public override bool ReduceReplicaLive { get { return false; } }
        public override bool UseAttempts { get { return false; } }
    }
}
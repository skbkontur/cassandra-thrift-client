using System;

namespace SKBKontur.Cassandra.CassandraClient.Exceptions
{
    public class CassandraClientAuthenticationException : CassandraClientException
    {
        public CassandraClientAuthenticationException(string message)
            : base(message)
        {
        }

        public CassandraClientAuthenticationException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public override bool IsCorruptConnection => false;
        public override bool ReduceReplicaLive => false;
        public override bool UseAttempts => false;
    }
}
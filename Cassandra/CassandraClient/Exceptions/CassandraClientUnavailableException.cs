using System;

namespace SKBKontur.Cassandra.CassandraClient.Exceptions
{
    public class CassandraClientUnavailableException : CassandraClientException
    {
        public CassandraClientUnavailableException(string message)
            : base(message)
        {
        }

        public CassandraClientUnavailableException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public override bool IsCorruptConnection { get { return false; } }
        public override bool ReduceReplicaLive { get { return false; } }
    }
}
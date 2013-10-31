using System;

namespace SKBKontur.Cassandra.CassandraClient.Exceptions
{
    public class CassandraClientSomethingNotFoundException : CassandraClientException
    {
        public CassandraClientSomethingNotFoundException(string message)
            : base(message)
        {
        }

        public CassandraClientSomethingNotFoundException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public override bool IsCorruptConnection { get { return false; } }
        public override bool ReduceReplicaLive { get { return false; } }
    }
}
using System;

namespace SkbKontur.Cassandra.ThriftClient.Exceptions
{
    public class CassandraClientSomethingNotFoundException : CassandraClientException
    {
        internal CassandraClientSomethingNotFoundException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public override bool IsCorruptConnection => false;
        public override bool ReduceReplicaLive => false;
    }
}
using System;

namespace SKBKontur.Cassandra.CassandraClient.Exceptions
{
    public class CassandraClientSchemaDisagreementException : CassandraClientException
    {
         public CassandraClientSchemaDisagreementException(string message)
            : base(message)
        {
        }

        public CassandraClientSchemaDisagreementException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public override bool IsCorruptConnection { get { return false; } }
        public override bool ReduceReplicaLive { get { return false; } }
        public override bool UseAttempts { get { return false; } }       
    }
}
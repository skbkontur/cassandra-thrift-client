using System;

namespace SKBKontur.Cassandra.CassandraClient.Exceptions
{
    public class CassandraClientSchemaDisagreementExceptionException : CassandraClientException
    {
         public CassandraClientSchemaDisagreementExceptionException(string message)
            : base(message)
        {
        }

        public CassandraClientSchemaDisagreementExceptionException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public override bool IsCorruptConnection { get { return false; } }
        public override bool ReduceReplicaLive { get { return false; } }        
        
    }
}
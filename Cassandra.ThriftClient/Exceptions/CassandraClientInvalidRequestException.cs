using System;

using Apache.Cassandra;

namespace SkbKontur.Cassandra.ThriftClient.Exceptions
{
    public class CassandraClientInvalidRequestException : CassandraClientException
    {
        internal CassandraClientInvalidRequestException(string message, InvalidRequestException innerException)
            : base($"{message}{Environment.NewLine}Why: {innerException.Why}", innerException)
        {
        }

        public override bool IsCorruptConnection => false;
        public override bool ReduceReplicaLive => false;
        public override bool UseAttempts => false;
    }
}
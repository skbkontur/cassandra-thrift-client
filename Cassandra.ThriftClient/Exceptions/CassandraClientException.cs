using System;

namespace SKBKontur.Cassandra.CassandraClient.Exceptions
{
    public abstract class CassandraClientException : Exception
    {
        protected CassandraClientException(string message)
            : base(message)
        {
        }

        protected CassandraClientException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public virtual bool IsCorruptConnection => true;

        public virtual bool ReduceReplicaLive => true;
        public virtual bool UseAttempts => true;
    }
}
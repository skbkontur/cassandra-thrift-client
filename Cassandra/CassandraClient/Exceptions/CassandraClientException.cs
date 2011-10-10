using System;

namespace SKBKontur.Cassandra.CassandraClient.Exceptions
{
    public class CassandraClientException : Exception
    {
        public CassandraClientException(string message)
            : base(message)
        {
        }

        public CassandraClientException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
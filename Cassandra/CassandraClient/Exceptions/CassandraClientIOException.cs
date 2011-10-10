using System;

namespace SKBKontur.Cassandra.CassandraClient.Exceptions
{
    public class CassandraClientIOException : CassandraClientException
    {
        public CassandraClientIOException(string message)
            : base(message)
        {
        }

        public CassandraClientIOException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
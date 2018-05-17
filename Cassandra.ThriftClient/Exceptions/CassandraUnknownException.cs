using System;

namespace SKBKontur.Cassandra.CassandraClient.Exceptions
{
    public class CassandraUnknownException : CassandraClientException
    {
        public CassandraUnknownException(string message)
            : base(message)
        {
        }

        public CassandraUnknownException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
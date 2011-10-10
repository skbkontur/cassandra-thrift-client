using System;

namespace SKBKontur.Cassandra.CassandraClient.Exceptions
{
    public class CassandraClientInvalidRequestException : CassandraClientException
    {
        public CassandraClientInvalidRequestException(string message)
            : base(message)
        {
        }

        public CassandraClientInvalidRequestException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
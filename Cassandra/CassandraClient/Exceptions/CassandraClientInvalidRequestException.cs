using System;

using Apache.Cassandra;

namespace SKBKontur.Cassandra.CassandraClient.Exceptions
{
    public class CassandraClientInvalidRequestException : CassandraClientException
    {
        public CassandraClientInvalidRequestException(string message)
            : base(message)
        {
        }

        public CassandraClientInvalidRequestException(string message, InvalidRequestException innerException)
            : base(message + Environment.NewLine + "Why: " + innerException.Why, innerException)
        {
        }
    }
}
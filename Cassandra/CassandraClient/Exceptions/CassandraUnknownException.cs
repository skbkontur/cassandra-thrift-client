using System;

namespace CassandraClient.Exceptions
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
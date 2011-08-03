using System;

namespace CassandraClient.Exceptions
{
    public class CassandraClientTransportException : CassandraClientException
    {
        public CassandraClientTransportException(string message)
            : base(message)
        {
        }

        public CassandraClientTransportException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
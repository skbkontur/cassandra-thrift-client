using System;

namespace CassandraClient.Exceptions
{
    public class CassandraClientApplicationException : CassandraClientException
    {
        public CassandraClientApplicationException(string message)
            : base(message)
        {
        }

        public CassandraClientApplicationException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
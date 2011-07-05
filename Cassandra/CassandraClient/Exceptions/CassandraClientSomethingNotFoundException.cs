using System;

namespace CassandraClient.Exceptions
{
    public class CassandraClientSomethingNotFoundException : CassandraClientException
    {
        public CassandraClientSomethingNotFoundException(string message)
            : base(message)
        {
        }

        public CassandraClientSomethingNotFoundException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
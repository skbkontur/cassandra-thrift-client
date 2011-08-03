using System;

namespace CassandraClient.Exceptions
{
    public class CassandraClientExceptionTypeRecognizer
    {
        public bool IsExceptionRetryable(Exception exception)
        {
            return exception is CassandraClientUnavailableException ||
                   exception is CassandraClientTimedOutException ||
                   exception is CassandraClientTransportException ||
                   exception is CassandraClientIOException;
        }
    }
}
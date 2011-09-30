using System;

namespace CassandraClient.Exceptions
{
    public class CassandraAttemptsException : CassandraClientException
    {
        public CassandraAttemptsException(int attempts)
            : base("Operation failed for " + attempts + " attempts")
        {
        }

        public CassandraAttemptsException(int attempts, Exception innerException)
            : base("Operation failed for " + attempts + " attempts", innerException)
        {
        }
    }
}
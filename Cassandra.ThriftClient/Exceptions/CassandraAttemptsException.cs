using System;

namespace SkbKontur.Cassandra.ThriftClient.Exceptions
{
    public class CassandraAttemptsException : CassandraClientException
    {
        internal CassandraAttemptsException(int attempts, Exception innerException)
            : base($"Operation failed for {attempts} attempts", innerException)
        {
        }
    }
}
using System;

namespace SKBKontur.Cassandra.CassandraClient.Exceptions
{
    public class CassandraAttemptsException : CassandraClientException
    {
        internal CassandraAttemptsException(int attempts, Exception innerException)
            : base($"Operation failed for {attempts} attempts", innerException)
        {
        }
    }
}
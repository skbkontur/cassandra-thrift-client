using System;

namespace SKBKontur.Cassandra.CassandraClient.Exceptions
{
    public class CassandraClientTransportException : CassandraClientException
    {
        internal CassandraClientTransportException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
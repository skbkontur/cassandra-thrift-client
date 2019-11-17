using System;

namespace SkbKontur.Cassandra.ThriftClient.Exceptions
{
    public class CassandraClientTransportException : CassandraClientException
    {
        internal CassandraClientTransportException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
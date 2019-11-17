using System;

namespace SkbKontur.Cassandra.ThriftClient.Exceptions
{
    public class CassandraClientIOException : CassandraClientException
    {
        internal CassandraClientIOException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
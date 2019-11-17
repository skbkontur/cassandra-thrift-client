using System;

namespace SkbKontur.Cassandra.ThriftClient.Exceptions
{
    public class CassandraUnknownException : CassandraClientException
    {
        internal CassandraUnknownException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
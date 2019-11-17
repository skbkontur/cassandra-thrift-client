using System;

namespace SkbKontur.Cassandra.ThriftClient.Exceptions
{
    public class CassandraClientTimedOutException : CassandraClientException
    {
        internal CassandraClientTimedOutException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
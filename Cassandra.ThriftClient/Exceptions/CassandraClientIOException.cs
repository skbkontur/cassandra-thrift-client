using System;

namespace SKBKontur.Cassandra.CassandraClient.Exceptions
{
    public class CassandraClientIOException : CassandraClientException
    {
        internal CassandraClientIOException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
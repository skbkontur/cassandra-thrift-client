using System;

namespace SKBKontur.Cassandra.CassandraClient.Exceptions
{
    public class CassandraUnknownException : CassandraClientException
    {
        internal CassandraUnknownException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
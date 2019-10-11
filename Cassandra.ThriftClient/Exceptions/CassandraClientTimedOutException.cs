using System;

namespace SKBKontur.Cassandra.CassandraClient.Exceptions
{
    public class CassandraClientTimedOutException : CassandraClientException
    {
        internal CassandraClientTimedOutException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
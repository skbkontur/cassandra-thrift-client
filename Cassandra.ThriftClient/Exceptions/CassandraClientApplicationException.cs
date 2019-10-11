using System;

using Thrift;

namespace SKBKontur.Cassandra.CassandraClient.Exceptions
{
    public class CassandraClientApplicationException : CassandraClientException
    {
        internal CassandraClientApplicationException(string message, TApplicationException innerException)
            : base($"{message}{Environment.NewLine}Type: {innerException.Type}", innerException)
        {
        }
    }
}
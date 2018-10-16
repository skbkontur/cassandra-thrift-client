using System;

using JetBrains.Annotations;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Exceptions
{
    public class CassandraClientInvalidResponseException : CassandraClientException
    {
        public CassandraClientInvalidResponseException(string message)
            : base($"Invalid Cassandra response: {message}")
        {
        }
    }
}
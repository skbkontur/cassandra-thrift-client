namespace SkbKontur.Cassandra.ThriftClient.Exceptions
{
    public class CassandraClientInvalidResponseException : CassandraClientException
    {
        internal CassandraClientInvalidResponseException(string message)
            : base($"Invalid Cassandra response: {message}")
        {
        }
    }
}
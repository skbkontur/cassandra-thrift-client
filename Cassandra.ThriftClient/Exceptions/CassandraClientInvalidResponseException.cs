namespace SKBKontur.Cassandra.CassandraClient.Exceptions
{
    public class CassandraClientInvalidResponseException : CassandraClientException
    {
        internal CassandraClientInvalidResponseException(string message)
            : base($"Invalid Cassandra response: {message}")
        {
        }
    }
}
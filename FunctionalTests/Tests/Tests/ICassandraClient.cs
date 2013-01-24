namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    public interface ICassandraClient
    {
        void Add(string keySpaceName, string columnFamilyName, string key, string columnName, byte[] columnValue,
                 long? timestamp = null, int? ttl = null);
    }
}
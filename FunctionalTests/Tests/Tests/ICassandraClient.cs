using System.Collections.Generic;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    public interface ICassandraClient
    {
        void RemoveAllKeyspaces();
        void AddKeyspace(string keySpaceName, string columnFamilyName);
        bool TryGetColumn(string keySpaceName, string columnFamilyName, string key, string columnName, out Column result);
        Column GetColumn(string keySpaceName, string columnFamilyName, string key, string columnName);
        void DeleteColumn(string keySpaceName, string columnFamilyName, string key, string columnName);

        void Add(string keySpaceName, string columnFamilyName, string key, string columnName, byte[] columnValue,
                 long? timestamp = null, int? ttl = null);

        void AddBatch(string keySpaceName, string columnFamilyName, string key, Column[] columns);
        void DeleteBatch(string keySpaceName, string columnFamilyName, string key, IEnumerable<string> columnNames, long? timestamp = null);

        Column[] GetRow(string keySpaceName, string columnFamilyName, string key, int count,
                        string startColumnName = null);
        Column[] GetRow(string keySpaceName, string columnFamilyName, string key);
        string[] GetKeys(string keySpaceName, string columnFamilyName);
        int GetCount(string keySpaceName, string columnFamilyName, string key);
        Dictionary<string, int> GetCounts(string keySpaceName, string columnFamilyName, IEnumerable<string> keys);
        void CheckConnections();
    }
}
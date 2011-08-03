using System.Collections.Generic;

using CassandraClient.Abstractions;

namespace Tests.Tests
{
    public interface ICassandraClient
    {
        void RemoveAllKeyspaces();
        void AddKeyspace(string keySpaceName, string columnFamilyName);
        bool TryGetColumn(string keySpaceName, string columnFamilyName, string key, string columnName, out Column result);
        Column GetColumn(string keySpaceName, string columnFamilyName, string key, string columnName);

        void Add(string keySpaceName, string columnFamilyName, string key, string columnName, byte[] columnValue,
                 long? timestamp = null, int? ttl = null);

        void AddBatch(string keySpaceName, string columnFamilyName, string key, Column[] columns);
        void DeleteBatch(string keySpaceName, string columnFamilyName, string key, IEnumerable<string> columnNames);

        Column[] GetRow(string keySpaceName, string columnFamilyName, string key, int count,
                        string startColumnName = null);
    }
}
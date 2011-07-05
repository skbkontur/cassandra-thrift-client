using CassandraClient.Abstractions;

namespace CassandraClient.StorageCore
{
    public interface IColumnFamilyRegistry
    {
        bool ContainsColumnFamily(string columnFamilyName);
        string[] GetColumnFamilyNames();
        IndexDefinition[] GetIndexDefinitions(string columnName);
    }
}
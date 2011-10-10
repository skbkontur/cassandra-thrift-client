using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.StorageCore
{
    public interface IColumnFamilyRegistry
    {
        bool ContainsColumnFamily(string columnFamilyName);
        string[] GetColumnFamilyNames();
        IndexDefinition[] GetIndexDefinitions(string columnName);
    }
}
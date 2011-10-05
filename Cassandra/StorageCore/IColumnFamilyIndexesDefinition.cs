using CassandraClient.Abstractions;

namespace StorageCore
{
    public interface IColumnFamilyIndexesDefinition
    {
        IndexDefinition[] IndexDefinitions { get; }
    }
}
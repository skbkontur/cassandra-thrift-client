using CassandraClient.Abstractions;

namespace CassandraClient.StorageCore
{
    public interface IColumnFamilyIndexesDefinition
    {
        IndexDefinition[] IndexDefinitions { get; }
    }
}
using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.StorageCore
{
    public interface IColumnFamilyIndexesDefinition
    {
        IndexDefinition[] IndexDefinitions { get; }
    }
}
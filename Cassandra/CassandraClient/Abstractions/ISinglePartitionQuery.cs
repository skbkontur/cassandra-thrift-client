using JetBrains.Annotations;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    internal interface ISinglePartitionQuery
    {
        [NotNull]
        string PartitionKey { get; }
    }
}
using JetBrains.Annotations;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    internal interface ISinglePartitionQuery
    {
        [NotNull]
        byte[] PartitionKey { get; }
    }
}
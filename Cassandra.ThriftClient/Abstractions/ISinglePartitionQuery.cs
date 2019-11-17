using JetBrains.Annotations;

namespace SkbKontur.Cassandra.ThriftClient.Abstractions
{
    internal interface ISinglePartitionQuery
    {
        [NotNull]
        byte[] PartitionKey { get; }
    }
}
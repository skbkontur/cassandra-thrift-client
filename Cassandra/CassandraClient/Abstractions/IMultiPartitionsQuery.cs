namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    internal interface IMultiPartitionsQuery
    {
        int QueriedPartitions { get; }
    }
}
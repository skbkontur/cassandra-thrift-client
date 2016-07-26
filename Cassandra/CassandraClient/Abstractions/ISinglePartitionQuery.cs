namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    internal interface ISinglePartitionQuery
    {
        string PartitionKey { get; }
    }
}
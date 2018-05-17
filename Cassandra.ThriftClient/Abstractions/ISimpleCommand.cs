namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    internal interface ISimpleCommand : ICommand
    {
        int QueriedPartitionsCount { get; }
    }
}
namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    internal interface ICommand
    {
        void Execute(Apache.Cassandra.Cassandra.Client client);
        string Name { get; }
        CommandContext CommandContext { get; }
        int QueriedPartitionsCount { get; }
    }
}
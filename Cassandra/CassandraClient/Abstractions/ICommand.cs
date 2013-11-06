namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    internal interface ICommand
    {
        void Execute(Apache.Cassandra.Cassandra.Client client);
        string Name { get; }
        bool IsFierce { get; }
        CommandContext CommandContext { get; }
    }
}
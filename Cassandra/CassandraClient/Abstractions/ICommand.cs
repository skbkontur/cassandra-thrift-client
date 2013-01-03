namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public interface ICommand
    {
        void Execute(Apache.Cassandra.Cassandra.Client client);
        string Name { get; }
        bool IsFierce { get; }
        CommandContext CommandContext { get; }
    }
}
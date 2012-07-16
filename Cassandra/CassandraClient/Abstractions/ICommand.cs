namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public interface ICommand
    {
        void Execute(Apache.Cassandra.Cassandra.Client client);
        ValidationResult Validate();
        string Keyspace { get; }
        bool IsFierce { get; }
    }
}
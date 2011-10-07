using Apache.Cassandra;

namespace CassandraClient.Abstractions
{
    public interface ICommand
    {
        void Execute(Cassandra.Client client);
        ValidationResult Validate();
        string Keyspace { get; }
        bool IsFierce { get; }
    }
}
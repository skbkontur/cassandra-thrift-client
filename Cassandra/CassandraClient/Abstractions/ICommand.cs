using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public interface ICommand
    {
        void Execute(Apache.Cassandra.Cassandra.Client client, ICassandraLogger logger);
        ValidationResult Validate(ICassandraLogger logger);
        string Keyspace { get; }
        bool IsFierce { get; }
    }
}
using Apache.Cassandra;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public interface IAquilesCommand
    {
        void Execute(Apache.Cassandra.Cassandra.Client cassandraClient);
        void ValidateInput();
        bool IsFierce { get; }
    }
}

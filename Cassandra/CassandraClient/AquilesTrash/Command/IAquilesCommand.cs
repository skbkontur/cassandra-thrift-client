using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public interface IAquilesCommand
    {
        void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ICassandraLogger logger);
        void ValidateInput(ICassandraLogger logger);
        bool IsFierce { get; }
    }
}

using Apache.Cassandra;

namespace CassandraClient.AquilesTrash.Command
{
    public interface IAquilesCommand
    {
        void Execute(Cassandra.Client cassandraClient);
        void ValidateInput();
        bool IsFierce { get; }
    }
}

using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public class RetrieveClusterPartitionerCommand : AbstractCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ICassandraLogger logger)
        {
            Partitioner = cassandraClient.describe_partitioner();
        }

        public override void ValidateInput(ICassandraLogger logger)
        {
        }

        public string Partitioner { get; private set; }
    }
}
using Apache.Cassandra;

namespace CassandraClient.AquilesTrash.Command
{
    public class RetrieveClusterPartitionerCommand : AbstractCommand
    {
        public override void Execute(Cassandra.Client cassandraClient)
        {
            Partitioner = cassandraClient.describe_partitioner();
        }

        public override void ValidateInput()
        {
        }

        public string Partitioner { get; private set; }
    }
}
using Apache.Cassandra;

namespace CassandraClient.AquilesTrash.Command
{
    public class RetrieveClusterPartitionerCommand : AbstractCommand, IAquilesCommand
    {
        public override void Execute(Cassandra.Client cassandraClient)
        {
            string partitioner = cassandraClient.describe_partitioner();
            Partitioner = partitioner;
        }

        public override void ValidateInput()
        {
        }

        public string Partitioner { get; private set; }
    }
}
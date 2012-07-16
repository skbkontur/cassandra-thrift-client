namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public class RetrieveClusterPartitionerCommand : AbstractCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Partitioner = cassandraClient.describe_partitioner();
        }

        public override void ValidateInput()
        {
        }

        public string Partitioner { get; private set; }
    }
}
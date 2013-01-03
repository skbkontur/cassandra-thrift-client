using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System.Read
{
    public class RetrieveClusterPartitionerCommand : CommandBase
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Partitioner = cassandraClient.describe_partitioner();
        }

        public override bool IsFierce { get { return true; } }
        public string Partitioner { get; private set; }
    }
}
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

namespace SKBKontur.Cassandra.CassandraClient.Commands.System.Read
{
    internal class RetrieveClusterPartitionerCommand : CommandBase
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Partitioner = cassandraClient.describe_partitioner();
        }

        public override bool IsFierce { get { return true; } }
        public string Partitioner { get; private set; }
    }
}
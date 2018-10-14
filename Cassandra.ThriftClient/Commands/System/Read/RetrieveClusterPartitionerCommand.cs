using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

using Vostok.Logging.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Commands.System.Read
{
    internal class RetrieveClusterPartitionerCommand : CommandBase, IFierceCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ILog logger)
        {
            Partitioner = cassandraClient.describe_partitioner();
        }

        public string Partitioner { get; private set; }
    }
}
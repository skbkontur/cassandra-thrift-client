using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Commands.Base;

using Vostok.Logging.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Commands.System.Read
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
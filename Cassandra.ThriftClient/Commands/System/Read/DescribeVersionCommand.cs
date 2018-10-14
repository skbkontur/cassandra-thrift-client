using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

using Vostok.Logging.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Commands.System.Read
{
    internal class DescribeVersionCommand : CommandBase, IFierceCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ILog logger)
        {
            Version = cassandraClient.describe_version();
        }

        public string Version { get; private set; }
    }
}
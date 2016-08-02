using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

namespace SKBKontur.Cassandra.CassandraClient.Commands.System.Read
{
    internal class DescribeVersionCommand : CommandBase, IFierceCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Version = cassandraClient.describe_version();
        }

        public string Version { get; private set; }
    }
}
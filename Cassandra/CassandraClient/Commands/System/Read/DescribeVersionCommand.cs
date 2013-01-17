using SKBKontur.Cassandra.CassandraClient.Commands.Base;

namespace SKBKontur.Cassandra.CassandraClient.Commands.System.Read
{
    internal class DescribeVersionCommand : CommandBase
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Version = cassandraClient.describe_version();
        }

        public override bool IsFierce { get { return true; } }
        public string Version { get; private set; }
    }
}
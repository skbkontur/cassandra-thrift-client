using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System.Read
{
    public class DescribeVersionCommand : CommandBase
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Version = cassandraClient.describe_version();
        }

        public override bool IsFierce { get { return true; } }
        public string Version { get; private set; }
    }
}
using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public class DescribeVersionCommand : AbstractCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ICassandraLogger logger)
        {
            Version = cassandraClient.describe_version();
        }

        public override void ValidateInput(ICassandraLogger logger)
        {
        }

        public string Keyspace { set; private get; }
        public string Version { get; private set; }
    }
}
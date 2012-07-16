namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public class DescribeVersionCommand : AbstractCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Version = cassandraClient.describe_version();
        }

        public override void ValidateInput()
        {
        }

        public string Keyspace { set; private get; }
        public string Version { get; private set; }
    }
}
using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public class DescribeKeyspaceCommand : AbstractCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            var keyspaceDescription = cassandraClient.describe_keyspace(Keyspace);
            KeyspaceInformation = ModelConverterHelper.Convert<AquilesKeyspace, KsDef>(keyspaceDescription);
        }

        public override void ValidateInput()
        {
        }

        public string Keyspace { set; private get; }
        public AquilesKeyspace KeyspaceInformation { get; private set; }
    }
}
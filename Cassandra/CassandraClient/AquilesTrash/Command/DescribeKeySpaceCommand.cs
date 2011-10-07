using Apache.Cassandra;

using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Model;

namespace CassandraClient.AquilesTrash.Command
{
    public class DescribeKeyspaceCommand : AbstractCommand
    {
        public override void Execute(Cassandra.Client cassandraClient)
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
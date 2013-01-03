using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System.Read
{
    public class DescribeKeyspaceCommand : KeyspaceDependantCommandBase
    {
        public DescribeKeyspaceCommand(string keyspace)
            : base(keyspace)
        {
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            var keyspaceDescription = cassandraClient.describe_keyspace(keyspace);
            KeyspaceInformation = ModelConverterHelper.Convert<AquilesKeyspace, KsDef>(keyspaceDescription);
        }

        public override bool IsFierce { get { return true; } }
        public AquilesKeyspace KeyspaceInformation { get; private set; }
    }
}
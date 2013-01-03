using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System.Write
{
    public class UpdateKeyspaceCommand : CommandBase
    {
        public UpdateKeyspaceCommand(AquilesKeyspace keyspaceDefinition)
        {
            this.keyspaceDefinition = keyspaceDefinition;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            var keyspace = ModelConverterHelper.Convert<AquilesKeyspace, KsDef>(keyspaceDefinition);
            Output = cassandraClient.system_update_keyspace(keyspace);
        }

        public string Output { get; private set; }
        public override bool IsFierce { get { return true; } }
        private readonly AquilesKeyspace keyspaceDefinition;
    }
}
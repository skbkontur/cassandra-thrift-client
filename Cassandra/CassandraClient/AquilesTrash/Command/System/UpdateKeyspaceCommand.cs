using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;
using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System
{
    public class UpdateKeyspaceCommand : AbstractCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ICassandraLogger logger)
        {
            var keyspace = ModelConverterHelper.Convert<AquilesKeyspace, KsDef>(KeyspaceDefinition);
            Output = cassandraClient.system_update_keyspace(keyspace);
        }

        public override void ValidateInput(ICassandraLogger logger)
        {
            KeyspaceDefinition.ValidateForInsertOperation();
        }

        public AquilesKeyspace KeyspaceDefinition { private get; set; }
        public string Output { get; private set; }
        public override bool IsFierce { get { return true; } }
    }
}
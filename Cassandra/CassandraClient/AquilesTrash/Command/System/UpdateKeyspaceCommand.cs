using Apache.Cassandra;

using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Model;

namespace CassandraClient.AquilesTrash.Command.System
{
    public class UpdateKeyspaceCommand : AbstractCommand
    {
        public override void Execute(Cassandra.Client cassandraClient)
        {
            var keyspace = ModelConverterHelper.Convert<AquilesKeyspace, KsDef>(KeyspaceDefinition);
            Output = cassandraClient.system_update_keyspace(keyspace);
        }

        public override void ValidateInput()
        {
            KeyspaceDefinition.ValidateForInsertOperation();
        }

        public AquilesKeyspace KeyspaceDefinition { private get; set; }
        public string Output { get; private set; }
    }
}
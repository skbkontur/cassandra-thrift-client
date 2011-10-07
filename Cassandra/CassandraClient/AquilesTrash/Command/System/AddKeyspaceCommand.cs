using Apache.Cassandra;

using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Model;

namespace CassandraClient.AquilesTrash.Command.System
{
    public class AddKeyspaceCommand : AbstractCommand
    {
        public override void Execute(Cassandra.Client cassandraClient)
        {
            var keyspace = ModelConverterHelper.Convert<AquilesKeyspace, KsDef>(KeyspaceDefinition);
            Output = cassandraClient.system_add_keyspace(keyspace);
        }

        public override void ValidateInput()
        {
            KeyspaceDefinition.ValidateForInsertOperation();
        }

        public AquilesKeyspace KeyspaceDefinition { get; set; }

        public string Output { get; private set; }
    }
}
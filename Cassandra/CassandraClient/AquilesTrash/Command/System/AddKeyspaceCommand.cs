using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System
{
    public class AddKeyspaceCommand : AbstractCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
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
        public override bool IsFierce { get { return true; } }
    }
}